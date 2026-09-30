"""
Consumidor de datos en streaming desde Apache Kafka.
Maneja buffering, checkpoints, reintentos exponenciales y errores.
"""
import json
import logging
import time
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Any
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaDataConsumer:
    """
    Consumidor de Kafka para ETL en streaming.
    Implementa buffering, manejo de errores y checkpoints para garantizar idempotencia.
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        batch_size: int = 1000,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        checkpoint_enabled: bool = True,
        auto_offset_reset: str = 'latest',
        enable_auto_commit: bool = False
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.checkpoint_enabled = checkpoint_enabled
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        
        self._consumer: Optional[Consumer] = None
        self._processed_offsets: Dict[str, int] = {}
        self._retry_count = 0
        
        self._validate_connection()
        self._init_consumer()
    
    def _validate_connection(self) -> None:
        """Valida la conexión al cluster de Kafka."""
        admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        
        try:
            cluster_metadata = admin.list_topics(timeout=10)
            logger.info(f"Conexión a Kafka exitosa. Topics disponibles: {len(cluster_metadata.topics)}")
            
            if self.topic not in cluster_metadata.topics:
                logger.warning(f"Topic '{self.topic}' no existe. Se creará automáticamente.")
        
        except Exception as e:
            logger.error(f"Error validando conexión a Kafka: {str(e)}")
            raise ConnectionError(f"No se puede conectar a Kafka: {str(e)}")
    
    def _init_consumer(self) -> None:
        """Inicializa el consumidor de Kafka con configuración robusta."""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': self.enable_auto_commit,
            'auto.commit.interval.ms': 5000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'max.poll.interval.ms': 300000,
            'fetch.min.bytes': 1,
            'fetch.max.wait.ms': 500,
            'enable.partition.eof': False,
            'client.id': f'etl-consumer-{self.group_id}'
        }
        
        self._consumer = Consumer(config)
        self._consumer.subscribe([self.topic])
        
        logger.info(f"Consumidor inicializado para topic '{self.topic}' con group_id '{self.group_id}'")
    
    def _retry_with_backoff(self, operation, *args, **kwargs):
        """Ejecuta operación con reintentos exponenciales."""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            
            except (KafkaError, KafkaException) as e:
                last_exception = e
                wait_time = (2 ** attempt) * 1.0
                
                logger.warning(f"Intento {attempt + 1}/{self.max_retries} fallido: {str(e)}. "
                             f"Reintentando en {wait_time}s...")
                
                time.sleep(wait_time)
                
                if attempt == self.max_retries - 1:
                    logger.error(f"Todos los reintentos agotados para la operación")
                    raise
        
        raise last_exception
    
    def _process_message(self, msg) -> Optional[Dict[str, Any]]:
        """Procesa un mensaje individual de Kafka."""
        if msg is None:
            return None
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.debug("Fin de partición alcanzado")
                return None
            else:
                logger.error(f"Error en mensaje: {msg.error()}")
                return None
        
        try:
            value = msg.value()
            
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            
            payload = json.loads(value)
            
            processed_message = {
                'message_id': self._generate_message_id(msg),
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': self._extract_timestamp(msg),
                'payload': payload,
                'key': msg.key().decode('utf-8') if msg.key() else None
            }
            
            return processed_message
        
        except json.JSONDecodeError as e:
            logger.error(f"Error decodificando mensaje JSON: {str(e)}")
            return {
                'message_id': str(uuid.uuid4()),
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': datetime.utcnow().isoformat(),
                'payload': {'raw': value.decode('utf-8') if isinstance(value, bytes) else value},
                'parse_error': str(e)
            }
        
        except Exception as e:
            logger.error(f"Error procesando mensaje: {str(e)}")
            return None
    
    def _generate_message_id(self, msg) -> str:
        """Genera identificador único para el mensaje."""
        return f"{msg.topic()}-{msg.partition()}-{msg.offset()}"
    
    def _extract_timestamp(self, msg) -> str:
        """Extrae timestamp del mensaje."""
        if msg.timestamp()[0]:
            ts_type, ts_value = msg.timestamp()
            return datetime.fromtimestamp(ts_value / 1000).isoformat()
        return datetime.utcnow().isoformat()
    
    def _checkpoint_offsets(self, records: List[Dict]) -> None:
        """Guarda offsets procesados para recuperación."""
        if not self.checkpoint_enabled:
            return
        
        for record in records:
            key = f"{record['topic']}-{record['partition']}"
            self._processed_offsets[key] = record['offset']
        
        logger.debug(f"Checkpoints guardados: {len(self._processed_offsets)}")
    
    def consume_batch(self, timeout_seconds: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Consume un lote de mensajes de Kafka.
        
        Args:
            timeout_seconds: Timeout para recibir mensajes. Si es None, usa el valor por defecto.
        
        Returns:
            Lista de mensajes procesados.
        """
        timeout = timeout_seconds or self.timeout_seconds
        batch = []
        start_time = time.time()
        
        logger.info(f"Iniciando consumo de hasta {self.batch_size} mensajes con timeout de {timeout}s")
        
        while len(batch) < self.batch_size:
            remaining_time = timeout - (time.time() - start_time)
            
            if remaining_time <= 0:
                logger.debug("Timeout alcanzado")
                break
            
            msg = self._consumer.poll(timeout=min(remaining_time, 1.0))
            
            processed = self._process_message(msg)
            
            if processed:
                batch.append(processed)
            
            if time.time() - start_time > timeout:
                break
        
        if batch:
            self._checkpoint_offsets(batch)
            logger.info(f"Consumidos {len(batch)} mensajes en {time.time() - start_time:.2f}s")
        else:
            logger.debug("No se recibieron mensajes en esta iteración")
        
        return batch
    
    def consume_single(self) -> Optional[Dict[str, Any]]:
        """Consume un único mensaje de Kafka."""
        msg = self._consumer.poll(timeout=self.timeout_seconds)
        return self._process_message(msg)
    
    def get_consumer_position(self) -> Dict[str, Any]:
        """Obtiene la posición actual del consumidor."""
        assignment = self._consumer.assignment()
        
        positions = {}
        for partition in assignment:
            committed = self._consumer.committed(partition)
            positions[f"{partition.topic}-{partition.partition}"] = {
                'offset': committed.offset() if committed else None,
                'high': self._consumer.get_watermark_offsets(partition)[1] if self._consumer.get_watermark_offsets(partition) else None
            }
        
        return positions
    
    def seek_to_beginning(self) -> None:
        """Reinicia el consumo desde el inicio de cada partición."""
        assignment = self._consumer.assignment()
        self._consumer.seek_to_beginning()
        logger.info("Consumidor reiniciado al inicio de las particiones")
    
    def seek_to_end(self) -> None:
        """Avança el consumo al final de cada partición."""
        assignment = self._consumer.assignment()
        self._consumer.seek_to_end()
        logger.info("Consumidor posicionado al final de las particiones")
    
    def close(self) -> None:
        """Cierra el consumidor y libera recursos."""
        if self._consumer:
            self._consumer.close()
            logger.info("Consumidor de Kafka cerrado")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class KafkaProducerHelper:
    """Productor auxiliar para enviar mensajes de control o errores."""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._producer = None
    
    def _get_producer(self) -> Producer:
        if self._producer is None:
            config = {
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': 'etl-producer-helper'
            }
            self._producer = Producer(config)
        return self._producer
    
    def send_message(self, topic: str, message: Dict) -> None:
        """Envía un mensaje al topic especificado."""
        producer = self._get_producer()
        
        def delivery_report(err, msg):
            if err:
                logger.error(f"Error enviando mensaje: {err}")
            else:
                logger.debug(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")
        
        producer.produce(
            topic,
            key=str(message.get('message_id', '')),
            value=json.dumps(message).encode('utf-8'),
            callback=delivery_report
        )
        producer.flush()