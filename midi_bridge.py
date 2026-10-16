#!/usr/bin/env python3
"""Live fx2lafw D1 (31250 baud, 8N1) -> existing loopMIDI output.

Python 3.10+. Only python-rtmidi is required; parsing/raw UART use the stdlib.
See README_NL.md. Raw captures MUST contain all eight channels, one byte/sample.
"""
from __future__ import annotations

import argparse
from collections import Counter, deque
import logging
import math
import os
from pathlib import Path
import queue
import re
import shutil
import signal
import subprocess
import sys
import threading
import time

LOG = logging.getLogger("midi-bridge")
BAUD = 31250
MAX_RESTARTS = 3
REALTIME = {0xF8, 0xFA, 0xFB, 0xFC, 0xFE, 0xFF}
COMMON_LENGTHS = {0xF1: 1, 0xF2: 2, 0xF3: 1, 0xF6: 0}


class BridgeError(RuntimeError):
    pass


class MidiParser:
    """Chunk-independent MIDI 1.0 parser. emit receives complete bytes messages.

    Real-time bytes are emitted immediately, even inside SysEx/partial messages.
    F9/FD are ignored without disturbing state. System common cancels running
    status. Incomplete, interrupted, oversized SysEx is discarded, never sent.
    """

    def __init__(self, emit, warn=None, max_sysex=65536, sysex_timeout=5.0):
        self.emit = emit
        self.warn = warn or (lambda message: None)
        self.max_sysex = max_sysex
        self.sysex_timeout = sysex_timeout
        self.counts = Counter()
        self.running = None
        self.status = None
        self.data = bytearray()
        self.sysex = None
        self.dropping_sysex = False
        self.last_sysex = 0.0

    def issue(self, kind, detail):
        self.counts[kind] += 1
        n = self.counts[kind]
        if n <= 3 or n % 100 == 0:
            self.warn(f"{detail} ({kind}: {n})")

    def reset(self, reason=None):
        if reason:
            self.issue("reset", reason)
        self.running = self.status = None
        self.data.clear()
        self.sysex = None
        self.dropping_sysex = False

    def _emit(self, message):
        self.emit(bytes(message))
        self.counts["messages"] += 1

    def tick(self, now=None):
        now = time.monotonic() if now is None else now
        if (self.sysex is not None or self.dropping_sysex) and self.sysex_timeout:
            if now - self.last_sysex > self.sysex_timeout:
                self.reset("SysEx aborted: no new SysEx byte for too long.")

    def feed(self, chunk, now=None):
        now = time.monotonic() if now is None else now
        self.tick(now)
        for value in chunk:
            self.counts["bytes"] += 1
            if value >= 0xF8:
                if value in REALTIME:
                    self._emit([value])
                else:
                    self.issue("undefined_realtime", f"Undefined real-time byte {value:02X} ignored.")
                continue

            if self.sysex is not None or self.dropping_sysex:
                if value == 0xF7:
                    if self.sysex is not None:
                        if len(self.sysex) + 1 <= self.max_sysex:
                            self.sysex.append(0xF7)
                            self._emit(self.sysex)
                        else:
                            self.issue("sysex_overflow", "SysEx exceeds the configured limit; discarded.")
                    self.sysex = None
                    self.dropping_sysex = False
                    continue
                if value < 0x80:
                    self.last_sysex = now
                    if self.sysex is not None:
                        if len(self.sysex) + 2 > self.max_sysex:  # reserve EOX
                            self.sysex = None
                            self.dropping_sysex = True
                            self.issue("sysex_overflow", "SysEx exceeds the configured limit; discarded.")
                        else:
                            self.sysex.append(value)
                    continue
                self.issue("sysex_interrupted", f"Incomplete SysEx interrupted by {value:02X}.")
                self.sysex = None
                self.dropping_sysex = False
                # Interpret the interrupting status normally below.

            if value & 0x80:
                if self.status is not None and self.data:
                    self.issue("partial", "Incomplete MIDI message replaced by a new status.")
                self.status = None
                self.data.clear()
                if value < 0xF0:
                    self.running = self.status = value
                else:
                    self.running = None
                    if value == 0xF0:
                        self.sysex = bytearray([0xF0])
                        self.last_sysex = now
                    elif value in COMMON_LENGTHS:
                        if COMMON_LENGTHS[value] == 0:
                            self._emit([value])
                        else:
                            self.status = value
                    else:
                        self.issue("undefined_common", f"Standalone/undefined status {value:02X} ignored.")
                continue

            if self.status is None:
                self.status = self.running
            if self.status is None:
                self.issue("orphan_data", f"Data {value:02X} without a MIDI status ignored.")
                continue
            self.data.append(value)
            needed = (1 if self.status & 0xF0 in (0xC0, 0xD0) else 2) if self.status < 0xF0 else COMMON_LENGTHS[self.status]
            if len(self.data) == needed:
                self._emit(bytes([self.status]) + self.data)
                self.status = None
                self.data.clear()

    def finish(self):
        if self.sysex is not None or self.dropping_sysex or self.data:
            self.issue("unfinished", "Incomplete message discarded on shutdown.")
        self.reset()


class RawUartDecoder:
    """Streaming 8N1 decoder: all 8 FX2 channels, bit N = physical DN.

    bytes.translate/find do the sample scanning in C. Only frames are processed
    in Python. A bounded tail preserves edges/frames across arbitrary pipe reads.
    Validates start/stop via majority of three samples around each bit centre.
    Requires an observed idle-high sample before the first falling edge.
    """

    def __init__(self, emit, on_error=None, samplerate=1_000_000, channel=1, invert=False):
        if samplerate / BAUD < 16:
            raise ValueError("At least 16 samples per MIDI bit are required.")
        self.emit = emit
        self.on_error = on_error or (lambda message: None)
        self.spb = samplerate / BAUD
        self.table = bytes(int(bool(i & (1 << channel))) ^ int(invert) for i in range(256))
        self.tail = b""
        self.skip = 0
        self.counts = Counter()

    @staticmethod
    def majority(buf, index):
        return int(buf[index - 1] + buf[index] + buf[index + 1] >= 2)

    def feed(self, raw):
        self.counts["samples"] += len(raw)
        buf = self.tail + raw.translate(self.table)
        pos = self.skip
        while True:
            edge = buf.find(b"\x01\x00", max(0, pos - 1))
            if edge < 0:
                self.tail = buf[-1:]
                self.skip = max(1, pos - len(buf) + 1)
                return
            start = edge + 1
            last = start + round(9.5 * self.spb)
            if last + 1 >= len(buf):
                self.tail = buf[edge:]
                self.skip = 1
                return
            if self.majority(buf, start + round(0.5 * self.spb)) != 0:
                self.counts["false_starts"] += 1
                pos = start + 1
                continue
            if self.majority(buf, last) != 1:
                self.counts["framing_errors"] += 1
                self.on_error("UART stop-bit error: MIDI parser resynchronized; check D1/inversion/GND.")
                # Do not search in the damaged byte's data bits.
                pos = last + 2
                continue
            value = 0
            for bit in range(8):
                value |= self.majority(buf, start + round((1.5 + bit) * self.spb)) << bit
            self.counts["bytes"] += 1
            self.emit(bytes([value]))
            # Search after the validated stop centre, NOT after nominal 10 bits.
            # A transmitter running 1% faster starts the next frame before that
            # nominal boundary; skipping a full frame would silently miss it.
            pos = last + 2


def find_sigrok(explicit=None):
    if explicit:
        resolved = shutil.which(explicit) or (str(Path(explicit).resolve()) if Path(explicit).is_file() else None)
        if resolved:
            return resolved
        raise BridgeError(f"sigrok-cli not found: {explicit}")
    found = shutil.which("sigrok-cli")
    if found:
        return found
    candidates = []
    for root_name in ("ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA"):
        root = os.environ.get(root_name)
        if root:
            for folder in ("sigrok", "sigrok-cli", "PulseView", "Programs/sigrok-cli", "Programs/PulseView"):
                candidates.extend((Path(root) / folder).glob("**/sigrok-cli.exe"))
    if candidates:
        return str(sorted(candidates)[0])
    raise BridgeError('Install sigrok-cli or provide --sigrok "C:\\path\\sigrok-cli.exe". See README_NL.md.')


def probe(command, timeout=20):
    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=timeout, check=False, creationflags=(subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0))
    except subprocess.TimeoutExpired as exc:
        raise BridgeError("sigrok-cli did not respond within 20 seconds. Close PulseView and reconnect the analyzer.") from exc
    except OSError as exc:
        raise BridgeError(f"Could not start sigrok-cli: {exc}") from exc
    out = result.stdout.decode("utf-8", errors="replace")
    err = result.stderr.decode("utf-8", errors="replace")
    if result.returncode:
        raise BridgeError(f"sigrok-cli exit {result.returncode}:\n{out}\n{err}\nCheck WinUSB, firmware, and that PulseView is closed.")
    if err.strip():
        LOG.info("sigrok-cli: %s", err.strip())
    return out


def parse_scan(text):
    devices = []
    for line in text.splitlines():
        match = re.match(r"\s*(fx2lafw[^\s]*)\s+-\s+.*?with\s+(\d+)\s+channels?:\s*(.+)$", line)
        if match:
            channels = match[3].split()
            if len(channels) != int(match[2]):
                raise BridgeError(f"Unrecognized channel list: {line}")
            devices.append((match[1], channels))
    return devices


def discover(executable, driver):
    text = probe([executable, "--driver", driver, "--scan"])
    LOG.info("USB-scan:\n%s", text.strip())
    devices = parse_scan(text)
    if not devices:
        raise BridgeError("No fx2lafw device found. Close PulseView and check USB/WinUSB and sigrok-firmware-fx2lafw.")
    if len(devices) != 1:
        raise BridgeError("Multiple analyzers found. Select one with --driver fx2lafw:conn=BUS.ADDRESS from the scan.")
    selected, names = devices[0]
    if len(names) != 8 or names not in ([str(i) for i in range(8)], [f"D{i}" for i in range(8)]):
        raise BridgeError(f"This bridge requires an 8-channel FX2 with 0..7 or D0..D7; found: {names}")
    return selected, names


def build_capture_command(executable, driver, names, args):
    command = [executable, "--driver", driver, "--config", f"samplerate={args.samplerate}",
               "--channels", ",".join(names), "--continuous", "--loglevel", "2"]
    if args.backend == "raw":
        # Enabling all 8 prevents assumptions about repacking disabled channels.
        command += ["--output-format", "binary"]
    else:
        decoder = (f"uart:rx={names[args.channel]}:baudrate={BAUD}:data_bits=8:parity=none:"
                   f"stop_bits=1:bit_order=lsb-first:invert_rx={'yes' if args.invert else 'no'}")
        command += ["--protocol-decoders", decoder, "--protocol-decoder-binary", "uart=rx"]
    return command


class Capture:
    """Unbuffered binary pipe; stderr drains separately; bounded, lossless queue.

    On queue overflow fail the entire capture; never drop samples and continue.
    Reader threads only transport bytes. MIDI output happens on the main thread.
    """

    def __init__(self, command, queue_size=128):
        self.command = command
        self.chunks = queue.Queue(maxsize=queue_size)
        self.stop_event = threading.Event()
        self.error = None
        self.stderr_tail = deque(maxlen=40)
        self.process = None
        self.threads = []

    def start(self):
        try:
            # sigrok --continuous watches stdin for "any key". DEVNULL/EOF can
            # stop acquisition immediately, so keep a private pipe OPEN.
            self.process = subprocess.Popen(self.command, stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0,
                                            creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0),
                                            start_new_session=(os.name != "nt"))
        except OSError as exc:
            raise BridgeError(f"Could not start capture: {exc}") from exc
        for target in (self._read_stdout, self._read_stderr):
            thread = threading.Thread(target=target, daemon=True)
            self.threads.append(thread)
            thread.start()
        return self

    def _put(self, data):
        try:
            self.chunks.put(data, timeout=0.5)
            return True
        except queue.Full:
            if not self.stop_event.is_set():
                self.error = "Capture buffer full. Stopped to prevent data loss; close busy applications or use --backend uart."
            return False

    def _read_stdout(self):
        try:
            while not self.stop_event.is_set():
                data = self.process.stdout.read(65536)  # FileIO: returns available bytes, not read-exactly
                if not self._put(data):
                    return
                if not data:
                    return
        except Exception as exc:
            if not self.stop_event.is_set():
                self.error = f"Error reading capture: {exc}"

    def _read_stderr(self):
        try:
            while not self.stop_event.is_set():
                line = self.process.stderr.readline(4096)
                if not line:
                    return
                text = line.decode("utf-8", errors="replace").rstrip()
                self.stderr_tail.append(text)
                LOG.warning("sigrok: %s", text)
        except (OSError, ValueError):
            pass

    def stop(self):
        self.stop_event.set()
        process = self.process
        if process is None:
            return
        if process.poll() is None:
            try:
                # Graceful sigrok "any key" stop, including Windows without a
                # console. Never use the parent's stdin (would steal Ctrl+C).
                process.stdin.write(b"\n")
                process.stdin.flush()
                process.wait(timeout=1.5)
            except (OSError, ValueError, subprocess.TimeoutExpired):
                pass
        if process.poll() is None:
            try:
                if os.name == "nt":
                    process.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    os.killpg(process.pid, signal.SIGINT)
                process.wait(timeout=1.5)
            except (OSError, subprocess.TimeoutExpired):
                if process.poll() is None:
                    process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
        for thread in self.threads:
            thread.join(timeout=2)
        for pipe in (process.stdin, process.stdout, process.stderr):
            if pipe:
                pipe.close()


def import_rtmidi():
    try:
        import rtmidi
        return rtmidi
    except ImportError as exc:
        raise BridgeError("python-rtmidi is missing. Run install.cmd or: python -m pip install -r requirements.txt") from exc


def make_midiout():
    rtmidi = import_rtmidi()
    try:
        if os.name == "nt":
            return rtmidi.MidiOut(rtmidi.API_WINDOWS_MM)
        return rtmidi.MidiOut()
    except Exception as exc:
        raise BridgeError(f"Could not start Windows MIDI: {exc}") from exc


def select_port(ports, name, index=None):
    if index is not None:
        if 0 <= index < len(ports):
            return index
        raise BridgeError(f"Port index {index} does not exist; available ports: {ports}")
    exact = [i for i, p in enumerate(ports) if p.casefold() == name.casefold()]
    matches = exact or [i for i, p in enumerate(ports) if name.casefold() in p.casefold()]
    if len(matches) == 1:
        return matches[0]
    raise BridgeError(f"MIDI port '{name}' is missing or not unique. Start loopMIDI and create this port.\nAvailable: {ports}\nUse --list-midi, --port, or --port-index.")


class MidiSink:
    def __init__(self, args):
        self.args = args
        self.output = None
        self.sent = 0
        if not args.dry_run:
            self.output = make_midiout()
            try:
                ports = self.output.get_ports()
                index = select_port(ports, args.port, args.port_index)
                self.output.open_port(index)
                LOG.info("MIDI output opened: %s", ports[index])
            except Exception:
                self.output.close_port()
                raise

    def send(self, message):
        if self.args.verbose or self.args.dry_run:
            LOG.info("MIDI %s", message.hex(" ").upper())
        if self.output:
            try:
                self.output.send_message(list(message))
            except Exception as exc:
                raise BridgeError(f"Failed to send MIDI: {exc}. Check that loopMIDI is still running.") from exc
        self.sent += 1

    def close(self):
        if self.output:
            try:
                if not self.args.no_panic:
                    for channel in range(16):
                        for controller in (64, 123, 120):
                            self.output.send_message([0xB0 | channel, controller, 0])
                time.sleep(0.1)  # WinMM can otherwise lose the final messages.
            except Exception as exc:
                LOG.warning("MIDI cleanup: %s", exc)
            finally:
                self.output.close_port()


def run_bridge(args, executable, driver, names, stop_requested):
    command = build_capture_command(executable, driver, names, args)
    LOG.info("Capture: %s", subprocess.list2cmdline(command))
    LOG.info("D%d / sigrok '%s', %d baud 8N1, %.0f samples/bit, backend=%s, inverted=%s",
             args.channel, names[args.channel], BAUD, args.samplerate / BAUD, args.backend, args.invert)
    if args.backend == "uart":
        LOG.warning("The UART backend does not report framing errors through -B; use raw for stop-bit validation.")
    sink = MidiSink(args)
    parser = MidiParser(sink.send, LOG.warning, args.max_sysex, args.sysex_timeout)
    uart = RawUartDecoder(parser.feed, parser.reset, args.samplerate, args.channel, args.invert) if args.backend == "raw" else None
    capture = Capture(command)
    began = last_data = last_stats = time.monotonic()
    warned_idle = False
    total = 0
    try:
        capture.start()
        LOG.info("Live capture started. Play MIDI; Ctrl+C stops and shuts everything down.")
        while not stop_requested.is_set():
            if capture.error:
                raise BridgeError(capture.error)
            now = time.monotonic()
            if args.duration and now - began >= args.duration:
                break
            try:
                data = capture.chunks.get(timeout=0.1)
            except queue.Empty:
                data = None
            if data == b"":
                try:
                    code = capture.process.wait(timeout=2)
                except subprocess.TimeoutExpired as exc:
                    raise BridgeError("sigrok closed stdout but is still running.") from exc
                raise BridgeError(f"Live capture stopped unexpectedly (exit {code}).\n" + "\n".join(capture.stderr_tail))
            if data:
                last_data = now
                total += len(data)
                if uart:
                    uart.feed(data)
                else:
                    parser.feed(data)
            parser.tick()
            if uart and now - last_data > args.capture_timeout:
                raise BridgeError("No raw samples from sigrok. Check USB/driver and close PulseView. Samples should also arrive during MIDI silence.")
            if not warned_idle and now - began > 10 and not parser.counts["bytes"]:
                warned_idle = True
                LOG.warning("No UART bytes yet. Play notes; check D1, common GND, and --invert. Silence can be normal in UART mode.")
            if now - last_stats >= args.stats_interval:
                sample_info = f", UART={dict(uart.counts)}" if uart else ""
                LOG.info("Status %.1fs: capture=%d bytes, wachtrij=%d, MIDI=%s%s", now - began,
                         total, capture.chunks.qsize(), dict(parser.counts), sample_info)
                if capture.chunks.qsize() > 32:
                    LOG.warning("Capture is falling behind. Try --backend uart; the queue must not keep growing.")
                last_stats = now
    finally:
        try:
            capture.stop()
        finally:
            parser.finish()
            sink.close()
            LOG.info("Stopped: %d MIDI messages, parser=%s%s", sink.sent, dict(parser.counts),
                     f", UART={dict(uart.counts)}" if uart else "")


def arguments(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sigrok", help="Full path to sigrok-cli.exe (otherwise search automatically)")
    p.add_argument("--driver", default="fx2lafw", help="For multiple devices: fx2lafw:conn=BUS.ADDRESS")
    p.add_argument("--backend", choices=("raw", "uart"), default="raw", help="raw validates frames; uart uses the sigrok decoder")
    p.add_argument("--channel", type=int, choices=range(8), default=1, help="Physical channel 0..7; default D1")
    p.add_argument("--samplerate", type=int, default=1_000_000, help="Samples/s, default 1000000; MIDI remains 31250 baud")
    p.add_argument("--invert", action="store_true", help="Use when the MIDI line is idle-low")
    p.add_argument("--port", default="LogicAnalyzer MIDI", help="Bestaande loopMIDI-poortnaam")
    p.add_argument("--port-index", type=int, help="Alternative: index from --list-midi")
    p.add_argument("--list-midi", action="store_true", help="List MIDI outputs without opening an analyzer")
    p.add_argument("--diagnose", action="store_true", help="Show version, USB scan, device options, and MIDI ports")
    p.add_argument("--dry-run", action="store_true", help="Capture and display MIDI without a MIDI port/dependency")
    p.add_argument("--verbose", action="store_true", help="Log every MIDI message (may affect timing)")
    p.add_argument("--log-file", help="Also save diagnostics to this UTF-8 text file")
    p.add_argument("--max-sysex", type=int, default=65536, help="Maximum bytes including F0/F7")
    p.add_argument("--sysex-timeout", type=float, default=5, help="Maximum SysEx silence in seconds; 0 disables it")
    p.add_argument("--capture-timeout", type=float, default=10, help="Stop when raw samples are absent for this many seconds")
    p.add_argument("--stats-interval", type=float, default=5, help="Seconds between status lines")
    p.add_argument("--duration", type=float, default=0, help="Stop after N seconds; 0 = unlimited")
    p.add_argument("--no-panic", action="store_true", help="Do not send sustain-off/all-notes-off/all-sound-off on shutdown")
    args = p.parse_args(argv)
    if args.samplerate < 500_000 or args.samplerate > 24_000_000:
        p.error("--samplerate must be between 500000 and 24000000 and supported by fx2lafw.")
    for key in ("sysex_timeout", "capture_timeout", "stats_interval", "duration"):
        value = getattr(args, key)
        if not math.isfinite(value) or value < 0 or (key in ("capture_timeout", "stats_interval") and value == 0):
            p.error(f"--{key.replace('_', '-')} has an invalid value.")
    if args.max_sysex < 2:
        p.error("--max-sysex must be at least 2.")
    if not args.driver.startswith("fx2lafw"):
        p.error("Only fx2lafw is supported.")
    return args


def list_midi():
    output = make_midiout()
    try:
        ports = output.get_ports()
        for i, name in enumerate(ports):
            print(f"[{i}] {name}", flush=True)
        if not ports:
            print("No MIDI outputs. Start loopMIDI and create 'LogicAnalyzer MIDI'.", flush=True)
    finally:
        output.close_port()


def main(argv=None):
    args = arguments(argv)
    handlers = [logging.StreamHandler()]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file, encoding="utf-8"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers)
    stop_requested = threading.Event()
    previous = signal.signal(signal.SIGINT, lambda *_: stop_requested.set())
    try:
        if args.list_midi:
            list_midi()
            return 0
        if not args.driver.startswith("fx2lafw"):
            raise BridgeError("Only fx2lafw is supported.")
        if args.diagnose:
            executable = find_sigrok(args.sigrok)
            LOG.info("sigrok-cli: %s", executable)
            driver, names = discover(executable, args.driver)
            print(probe([executable, "--version"]))
            print(probe([executable, "--driver", driver, "--show"]))
            list_midi()
            return 0
        restarts = 0
        while not stop_requested.is_set():
            try:
                executable = find_sigrok(args.sigrok)
                LOG.info("sigrok-cli: %s", executable)
                driver, names = discover(executable, args.driver)
                run_bridge(args, executable, driver, names, stop_requested)
                break
            except BridgeError as exc:
                if stop_requested.is_set() or restarts >= MAX_RESTARTS:
                    raise
                restarts += 1
                LOG.warning("Bridge failed: %s Restarting (%d/%d).", exc, restarts, MAX_RESTARTS)
        return 0
    except BridgeError as exc:
        LOG.error("%s", exc)
        return 1
    except Exception:
        LOG.exception("Unexpected error (save this diagnostic information).")
        return 1
    finally:
        signal.signal(signal.SIGINT, previous)


if __name__ == "__main__":
    raise SystemExit(main())
