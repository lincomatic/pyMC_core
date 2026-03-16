import asyncio
import json
import logging
import configparser
import aiomqtt
from aiomqtt import ProtocolVersion
import ssl
from typing import Any, Callable, Optional

# Assuming .base is available in your environment
from .base import LoRaRadio

logger = logging.getLogger("MQTTRadio")

class PktInfo:
    def __init__(self, iata: str, observer: str):
        self.iata = iata
        self.observer = observer

class MQTTRadio(LoRaRadio):
    def __init__(self, config_file="mqtt_config.ini"):
        self._rx_queue = asyncio.Queue()
        self.rx_callback = None
        
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        self.broker_url = self.config.get("mqtt", "mqtt_url")
        self.broker_port = self.config.getint("mqtt", "mqtt_port")
        self.username = self.config.get("mqtt", "mqtt_username")
        self.password = self.config.get("mqtt", "mqtt_password")
        
        topics_string = self.config.get("mqtt", "mqtt_topics")
        self.topics = [topic.strip() for topic in topics_string.split(',')]
        
        self.use_ws = self.config.get("mqtt", "use_websockets", fallback='n') == 'y'
        self.tls_insecure = self.config.get("mqtt", "tls_insecure", fallback='n') == 'y'
        logger.info("**** creating start task")
        self._radio_task = asyncio.create_task(self.start())

    def begin(self):
        """Initialise the radio module."""
        return

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

    async def start(self):
        """Infinite loop that handles connection and automatic resubscription."""
        logger.info("*** MQTT start")
        while True:
            try:
                if self.tls_insecure:
                    context = ssl._create_unverified_context()
                else:
                    context = ssl.create_default_context()

                logger.info("*** try connect to broker")
                async with aiomqtt.Client(
                    hostname=self.broker_url,
                    port=self.broker_port,
                    username=self.username,
                    password=self.password,
                    protocol=ProtocolVersion.V5,
                    transport="websockets" if self.use_ws else "tcp",
                    tls_context=context if not self.use_ws else None
                ) as client:
                    logger.info(f"Connected to MQTT broker at {self.broker_url}")
                    
                    for topic in self.topics:
                        await client.subscribe(topic,qos=1)
                        logger.info(f"Subscribed to {topic}")

                    # Run message listener and queue processor together
                    await asyncio.gather(
                        self._listen_messages(client),
                        self._process_rx_queue()
                    )

            except aiomqtt.MqttError as e:
                logger.error(f"MQTT Error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error in MQTT loop: {e}")
                await asyncio.sleep(5)

    async def _listen_messages(self, client):
        async for message in client.messages:
            await self._handle_message(message)

    async def _handle_message(self, msg):
        topic = str(msg.topic)
        iata, observer = "unk", "unk"

        try:
            parts = topic.split("/")
            if "meshcore" in parts:
                idx = parts.index("meshcore")
                if idx + 1 < len(parts): iata = parts[idx + 1].upper()
                if idx + 2 < len(parts): observer = parts[idx + 2][:4].lower()

            payload = msg.payload.decode('utf-8')
            jsondata = json.loads(payload)
            rawstr = jsondata.get("raw", "")
            
            if rawstr:
                new_raw = bytes.fromhex(rawstr)
                pktinfo = PktInfo(iata=iata, observer=observer)
                logger.info(f"**** queuing rx {self._rx_queue.qsize()}")
                await self._rx_queue.put((new_raw, pktinfo))
        except Exception as e:
            logger.warning(f"Message parsing failed: {e}")

    async def _process_rx_queue(self):
        while True:
            packet, pktinfo = await self._rx_queue.get()
            if self.rx_callback:
                try:
                    logger.info(f"**** calling rx callback {self._rx_queue.qsize()}")
                    if asyncio.iscoroutinefunction(self.rx_callback):
                        await self.rx_callback(packet, pktinfo)
                    else:
                        self.rx_callback(packet, pktinfo)
                except Exception as e:
                    logger.error(f"Callback error: {e}")
            self._rx_queue.task_done()

    def set_rx_callback(self, callback: Callable):
        self.rx_callback = callback

