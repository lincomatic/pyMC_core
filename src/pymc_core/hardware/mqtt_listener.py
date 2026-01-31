import asyncio
import json
import logging
import time
import configparser
import paho.mqtt.client as mqtt

from typing import Any, Callable, Dict, Optional
from .base import LoRaRadio

logger = logging.getLogger("MQTTRadio")


class MQTTRadio(LoRaRadio):
    def __init__(self, config_file="mqtt_config.ini"):
        self._rx_event = asyncio.Event()
        self._event_loop = None;
        
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

        # Set up MQTT client
        if self.config.get("mqtt", "use_websockets",fallback='n') == 'y':
            self.client = mqtt.Client(transport="websockets")
        else:
            self.client = mqtt.Client()
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
    
    def on_connect(self, client, userdata, flags, rc):
        """Callback for when client connects to broker"""
        if rc == 0:
            logger.info("Successfully connected to MQTT broker")
            # Subscribe to all topics
            for topic in self.topics:
                client.subscribe(topic)
                logger.info(f"Subscribed to topic: {topic}")
        else:
            logger.error(f"Failed to connect to broker, return code {rc}")

    def on_message(self, client, userdata, msg):
        """Callback for when a message is received"""
        topic = msg.topic
        payload = msg.payload.decode('utf-8')
        jsondata = json.loads(payload)
        self.raw = bytes.fromhex(jsondata.get("raw",""))

        # Log to console and file
        logger.info(f"Received message from topic: {topic}")
        logger.info(f"Packet length: {len(self.raw)} bytes")

        # Check if RX task is dead and restart it
        if (
            not hasattr(self, "_rx_task")
            or self._rx_task is None
            or self._rx_task.done()
        ):
            try:
                loop = asyncio.get_running_loop()
                self._rx_task = loop.create_task(self._rx_background_task())
                logger.warning("[RX] Restarted dead RX task")
                return False  # Was dead, now restarted
            except Exception:
                logger.warning("[RX] Failed to restart dead RX task")
                return False  # Failed to restart

        self._rx_event.set()




        # Log structured data
        #self.log_message_data(topic, payload)

    def on_disconnect(self, client, userdata, rc):
        """Callback for when client disconnects from broker"""
        if rc != 0:
            logger.warning(f"Unexpected disconnection from broker (rc={rc})")
        else:
            logger.info("Disconnected from broker")

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
        while True:
            try:
                await self._rx_event.wait()
                logger.info(f"*****calling RX callback****")
                self.rx_callback(self.raw)
                self._rx_event.clear()
            except Exception as e:
                logger.warning(f"RX background task exception {e}")
                
        logger.info("EXITING RX TASK")

    def set_rx_callback(self, callback: Callable[[bytes], None]):
        """
        Set the RX callback function

        Args:
            callback: Function to call when a frame is received
        """
        self.rx_callback = callback
        
        try:
            loop = asyncio.get_running_loop()
            self._event_loop = loop;
            self._rx_task = loop.create_task(self._rx_background_task())
        except RuntimeError:
            logger.debug("No event loop available for RX task startup")
        except Exception as e:
            logger.warning(f"Failed to start delayed RX IRQ background handler: {e}")
        
        logger.debug("RX callback set")
