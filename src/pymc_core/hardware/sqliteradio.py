import asyncio
import sqlite3
import logging
import os
from pathlib import Path
from typing import Callable, Optional

# Assuming .base is available in your environment
from .base import LoRaRadio

logger = logging.getLogger("SQLiteRadio")

class PktInfo:
    def __init__(self, iata: str, observer: str):
        self.iata = iata
        self.observer = observer

class SQLiteRadio(LoRaRadio):
    def __init__(self, db_path: str = "/opt/mqtt-mc-ingestor/meshcore.db", poll_interval: float = 0.1):
        """
        Initialize SQLite-based radio listener.
        
        Args:
            db_path: Path to the SQLite database file (default: /opt/mqtt-mc-ingestor/meshcore.db)
            poll_interval: How often to check for new packets (in seconds)
        """
        self._rx_queue = asyncio.Queue()
        self.rx_callback = None
        self.db_path = db_path
        self.poll_interval = poll_interval
        self._last_processed_id = 0
        self._running = False
        self._event_loop = asyncio.get_event_loop()
        
        logger.info(f"**** Initializing SQLite radio with database: {db_path}")
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
        """Infinite loop that monitors the database for new packets."""
        logger.info("*** SQLite radio start")
        self._running = True
        
        # Initialize to current max ID to skip existing rows
        await asyncio.get_event_loop().run_in_executor(
            None, self._initialize_last_id
        )
        
        while self._running:
            try:
                # Check if database exists
                if not os.path.exists(self.db_path):
                    logger.warning(f"Database not found at {self.db_path}. Waiting...")
                    await asyncio.sleep(5)
                    continue
                
                # Run database poller and queue processor together
                await asyncio.gather(
                    self._poll_database(),
                    self._process_rx_queue()
                )
                
            except Exception as e:
                logger.error(f"Unexpected error in SQLite loop: {e}")
                await asyncio.sleep(5)

    async def _poll_database(self):
        """Poll the database for new packets."""
        logger.info("*** Starting database polling")
        
        while self._running:
            try:
                # Run database query in executor to avoid blocking
                await asyncio.get_event_loop().run_in_executor(
                    None, self._fetch_new_packets
                )
                await asyncio.sleep(self.poll_interval)
                
            except Exception as e:
                logger.error(f"Database polling error: {e}")
                await asyncio.sleep(1)

    def _initialize_last_id(self):
        """Initialize last_processed_id to current max ID (runs in executor)."""
        try:
            if not os.path.exists(self.db_path):
                logger.info("Database doesn't exist yet, starting from ID 0")
                return
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(id) FROM packets")
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0] is not None:
                self._last_processed_id = result[0]
                logger.info(f"Initialized to skip existing rows, starting after ID {self._last_processed_id}")
            else:
                logger.info("No existing rows in database, starting from ID 0")
                
        except sqlite3.Error as e:
            logger.error(f"Error initializing last ID: {e}")
        except Exception as e:
            logger.error(f"Unexpected error initializing last ID: {e}")

    def _fetch_new_packets(self):
        """Fetch new packets from the database (runs in executor)."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Query for packets with ID greater than last processed
            cursor.execute(
                """
                SELECT id, timestamp, iata, observer, raw, 
                       route_type, payload_type, version
                FROM packets 
                WHERE id > ?
                ORDER BY id ASC
                """,
                (self._last_processed_id,)
            )
            
            rows = cursor.fetchall()
            
            for row in rows:
                try:
                    # Extract data from database row
                    packet_id = row['id']
                    iata = row['iata']
                    observer = row['observer']
                    raw_hex = row['raw']
                    
                    # Convert hex string to bytes
                    raw_bytes = bytes.fromhex(raw_hex)
                    
                    # Create packet info
                    pktinfo = PktInfo(iata=iata, observer=observer)
                    
                    # Queue the packet (use asyncio.run_coroutine_threadsafe since we're in executor)
                    asyncio.run_coroutine_threadsafe(
                        self._rx_queue.put((raw_bytes, pktinfo)),
                        self._event_loop
                    )
                    
                    logger.info(f"**** Queued packet ID {packet_id} from {iata}/{observer}")
                    
                    # Update last processed ID
                    self._last_processed_id = packet_id
                    
                except Exception as e:
                    logger.warning(f"Failed to process packet ID {row['id']}: {e}")
            
            conn.close()
            
        except sqlite3.Error as e:
            logger.error(f"SQLite error: {e}")
        except Exception as e:
            logger.error(f"Error fetching packets: {e}")

    async def _process_rx_queue(self):
        """Process packets from the receive queue."""
        while self._running:
            try:
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
            except Exception as e:
                logger.error(f"Queue processing error: {e}")
                await asyncio.sleep(0.1)

    def set_rx_callback(self, callback: Callable):
        """Set the callback function to be called when packets are received."""
        self.rx_callback = callback
    
    def stop(self):
        """Stop the radio listener."""
        logger.info("*** Stopping SQLite radio")
        self._running = False

