import asyncio
import json
import logging
import time
import configparser
import threading
from collections import deque
import paho.mqtt.client as mqtt

from typing import Any, Callable, Dict, Optional
from .base import LoRaRadio

logger = logging.getLogger("MQTTRadio")


class PktInfo:
    """Metadata about a received packet"""
    def __init__(self, iata: str, observer: str):
        self.iata = iata
        self.observer = observer


class MQTTRadio(LoRaRadio):
    def __init__(self, config_file="mqtt_config.ini"):
        self._rx_queue = asyncio.Queue()
        self._event_loop = None
        
        logger.info(f"Config file: {config_file}")
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Get MQTT settings from config
        self.broker_url = self.config.get("mqtt", "mqtt_url")
        self.broker_port = self.config.getint("mqtt", "mqtt_port")
        self.username = self.config.get("mqtt", "mqtt_username")
        self.password = self.config.get("mqtt", "mqtt_password")

        topics_string = self.config.get("mqtt", "mqtt_topics")
        self.topics = [topic.strip() for topic in topics_string.split(',')]

        # Set up MQTT client (use Callback API v2 + MQTT v5)
        if self.config.get("mqtt", "use_websockets",fallback='n') == 'y':
            self.client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                protocol=mqtt.MQTTv5,
                transport="websockets",
            )
        else:
            self.client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                protocol=mqtt.MQTTv5,
            )
        self.client.username_pw_set(self.username, self.password)

        # Set up callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        if self.config.get("mqtt", "tls_insecure",fallback='n') == 'y':
            self.client.tls_set(cert_reqs=mqtt.ssl.CERT_NONE)
            self.client.tls_insecure_set(True)
        else:
            self.client.tls_set()


        logger.info(f"Initialized MQTT subscriber for broker: {self.broker_url}:{self.broker_port}")
        logger.info(f"Subscribed topics: {self.topics}")
        self.begin()


    def begin(self):
        """Initialise the radio module."""
        self.client.connect(self.broker_url, self.broker_port, 60)
        self.client.loop_start()

    async def send(self, data: bytes):
        """Send a packet asynchronously. Returns transmission metadata dict or None."""
        return None

    async def wait_for_rx(self) -> bytes:
        """Wait for a packet to be received asynchronously."""
        data = b""
        return data

    def sleep(self):
        """Put the radio into low-power mode."""
        return None

    def get_last_rssi(self) -> int:
        """Return last received RSSI in dBm."""
        return 0

    def get_last_snr(self) -> float:
        """Return last received SNR in dB."""
        return 0.0
    
    def get_noise_floor(self) -> Optional[float]:
        """
        Get current noise floor in dBm.
        Returns properly sampled noise floor from background measurements.
        """
        return 0.0
    
    def on_connect(self, client, userdata, connect_flags, reason, properties):
        """Callback for when client connects to broker (Callback API v2 / MQTT v5).

        Args:
            connect_flags: ConnectFlags (session present, etc.)
            reason: ReasonCode (0 == success)
            properties: MQTT v5 Properties or None
        """
        # reason is a ReasonCode instance; compare to 0 for success
        if reason == 0:
            logger.info(f"Successfully connected to MQTT broker (reason={reason})")
            # Subscribe to all topics
            for topic in self.topics:
                client.subscribe(topic)
                logger.info(f"Subscribed to topic: {topic}")
        else:
            logger.error(f"Failed to connect to broker, reason={reason}")

    def on_message(self, client, userdata, msg):
        """Callback for when a message is received"""
        topic = msg.topic

        # Parse IATA and pubkey from topic of form: meshcore/<IATA>/pubkey/packets
        iata = None
        observer = None
        try:
            parts = topic.split("/")
            if len(parts) >= 3 and parts[0] == "meshcore":
                iata = parts[1]
                pubkey = parts[2]
            else:
                # tolerant fallback: find "meshcore" and take the next segments if available
                if "meshcore" in parts:
                    idx = parts.index("meshcore")
                    if idx + 1 < len(parts):
                        iata = parts[idx + 1]
                    if idx + 2 < len(parts):
                        pubkey = parts[idx + 2]
        except Exception:
            iata = None
            pubkey = None

        if iata == None:
            iata = "unk"
        else:
            iata = iata.upper()

        if pubkey is None:
            observer = "unk"
        else:
            observer = pubkey[:4].lower()

        self.last_iata = iata
        self.observer = observer
        
        payload = msg.payload.decode('utf-8')
        jsondata = json.loads(payload)
        rawstr = jsondata.get("raw", "")
        if not isinstance(rawstr, str) or rawstr == "":
            logger.info("Ignoring empty packet")
            return
        new_raw = bytes.fromhex(rawstr)

        # Enqueue packet for async processing
        pktinfo = PktInfo(iata=iata, observer=observer)
        item = (new_raw, pktinfo)
        if self._event_loop is not None and self._rx_queue is not None:
            try:
                self._event_loop.call_soon_threadsafe(self._enqueue_rx, item)
            except Exception as e:
                logger.warning(f"[RX] Failed to enqueue in async queue: {e}")
                return
        else:
            # No queue ready yet - drop the packet
            logger.debug("[RX] Dropping packet received before set_rx_callback()")
            return

        logger.info(f"rx from topic: {topic} (iata={iata}, obs={observer})")
        logger.info(
            f"Packet length: {len(new_raw)} bytes; queued packets: {self._rx_queue.qsize()}"
        )

        # Check if RX task is dead and restart it
        task_was_dead = False
        if (
            not hasattr(self, "_rx_task")
            or self._rx_task is None
            or self._rx_task.done()
        ):
            task_was_dead = True
            try:
                # If we have an event loop reference, create the task there thread-safely
                if hasattr(self, "_event_loop") and self._event_loop is not None:
                    def _start_rx_task():
                        try:
                            # Only restart if still not running
                            if (not hasattr(self, "_rx_task")
                                or self._rx_task is None
                                or self._rx_task.done()):
                                self._rx_task = asyncio.create_task(self._rx_background_task())
                                logger.warning("[RX] Restarted dead RX task")
                        except Exception as e:
                            logger.warning(f"[RX] Failed to start RX task inside loop: {e}")

                    self._event_loop.call_soon_threadsafe(_start_rx_task)
                else:
                    # Fallback: try to start from current thread (may fail)
                    try:
                        loop = asyncio.get_running_loop()
                        self._rx_task = loop.create_task(self._rx_background_task())
                        logger.warning("[RX] Restarted dead RX task")
                    except RuntimeError:
                        logger.warning("[RX] Failed to restart task: no event loop in MQTT thread")
            except Exception:
                logger.warning("[RX] Failed to restart dead RX task")

        # No explicit wakeup needed; queue put schedules work in the event loop




        # Log structured data
        #self.log_message_data(topic, payload)

    def on_disconnect(self, client, userdata, disconnect_flags, reason, properties):
        """Callback for when client disconnects from broker (Callback API v2 / MQTT v5)."""
        # reason is a ReasonCode instance; success == 0
        if reason == 0:
            logger.info("Disconnected from broker")
        else:
            logger.warning(f"Unexpected disconnection from broker (reason={reason})")

    def start(self):
        """Start the MQTT subscriber"""
        try:
            logger.info(f"Connecting to MQTT broker at {self.broker_url}:{self.broker_port}")
            self.client.connect(self.broker_url, self.broker_port, 60)

            # Start the loop to process callbacks
            logger.info("Starting MQTT subscriber loop...")
            logger.info("Press Ctrl+C to stop")
            self.client.loop_forever()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("MQTT subscriber stopped")
        except Exception as e:
            logger.error(f"Error in MQTT subscriber: {e}")
            raise

    async def _rx_background_task(self):
        """Background task that processes incoming packets from the FIFO queue.
        
        Continuously waits for packets on the async queue and drains any backlog.
        """
        while True:
            try:
                packet, pktinfo = await self._rx_queue.get()
                qs = self._rx_queue.qsize()
                try:
                    logger.info(f"*** RX callback {qs}***")
                    self.rx_callback(packet, pktinfo)
                except Exception as e:
                    logger.warning(f"[RX] Callback exception: {e}")
                self._rx_queue.task_done()

                # Drain backlog quickly without blocking
#                while True:
#                    try:
#                     except asyncio.QueueEmpty:
#                        break
#                    try:
#                        logger.info("*** RX callback drain ***")
#                        self.rx_callback(packet, pktinfo)
#                    except Exception as e:
#                        logger.warning(f"[RX] Callback exception: {e}")
                
            except asyncio.CancelledError:
                logger.info("RX background task cancelled")
                break
            except Exception as e:
                logger.warning(f"[RX] Background task error: {e}")
                # Brief pause before retry to avoid busy-loop on persistent errors
                await asyncio.sleep(0.1)
                continue
        
        logger.info("EXITING RX TASK")

    def set_rx_callback(self, callback: Callable[[bytes, PktInfo], None]):
        """
        Set the RX callback function

        Args:
            callback: Function to call when a frame is received
        """
        self.rx_callback = callback
        
        try:
            loop = asyncio.get_running_loop()
            self._event_loop = loop

            if not hasattr(self, "_rx_task") or self._rx_task is None or self._rx_task.done():
                self._rx_task = loop.create_task(self._rx_background_task())
        except RuntimeError:
            logger.debug("No event loop available for RX task startup")
        except Exception as e:
            logger.warning(f"Failed to start delayed RX IRQ background handler: {e}")
        
        logger.debug("RX callback set")

    def _enqueue_rx(self, item: tuple[bytes, PktInfo]) -> None:
        """Enqueue an RX item (runs on event loop)."""
        try:
            if self._rx_queue is None:
                return
            self._rx_queue.put_nowait(item)
        except Exception as e:
            logger.warning(f"[RX] Failed to enqueue packet in loop: {e}")
