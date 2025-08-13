const Redis = require('ioredis');
const { Kafka } = require('kafkajs');

// Conexión a Redis (servicio en Docker)
const redis = new Redis({
  host: 'localhost', // nombre del servicio Redis en Docker Compose
  port: 6379
});

redis.on('connect', () => {
  console.log('✅ Conectado a Redis');
});

redis.on('error', (err) => {
  console.error('❌ Error en Redis:', err);
});

// Kafka configuration
const kafka = new Kafka({
  clientId: 'cart-consumer-service',
  brokers: ['localhost:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

const consumer = kafka.consumer({ 
  groupId: 'shopping-cart-redis-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000
});

async function iniciarServicio() {
  try {
    // Conectar consumer de Kafka
    await consumer.connect();
    console.log('✅ Conectado a Kafka');
    
    // Suscribirse al topic exacto
    await consumer.subscribe({ 
      topic: 'email-cart-requests',
      fromBeginning: false
    });
    
    console.log('🔄 Esperando mensajes del carrito...');

    // Procesar mensajes
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const cartData = JSON.parse(message.value.toString());
          const cartHeaderId = message.key.toString();

          console.log(`📦 Mensaje recibido - Topic: ${topic}, Partition: ${partition}, Offset: ${message.offset}`);
          console.log(`🛒 Cart ID: ${cartHeaderId}`);

          // Guardar carrito en Redis con TTL de 1 hora
          const redisKey = `cart:${cartHeaderId}`;
          await redis.set(redisKey, JSON.stringify(cartData), 'EX', 3600);

          console.log(`✅ Carrito guardado en Redis: ${redisKey}`);
          console.log(`   - Usuario: ${cartData.CartHeader.UserId}`);
          console.log(`   - Total: $${cartData.CartHeader.CartTotal}`);
          console.log(`   - Items: ${cartData.CartDetailsDtos ? cartData.CartDetailsDtos.length : 0} productos`);
          console.log(`   - Kafka Offset: ${message.offset}`);
          console.log('-----------------------------------');

        } catch (error) {
          console.error('❌ Error procesando mensaje del carrito:', error);
          console.error('Mensaje original:', message.value.toString());

          // Guardar errores en Redis también (opcional)
          try {
            const errorKey = `cart_error:${Date.now()}`;
            await redis.set(errorKey, JSON.stringify({
              error: error.message,
              original_message: message.value.toString(),
              kafka_metadata: {
                topic,
                partition,
                offset: message.offset,
                timestamp: message.timestamp
              },
              error_timestamp: new Date()
            }), 'EX', 3600);
          } catch (logError) {
            console.error('❌ Error guardando log de error en Redis:', logError);
          }
        }
      },
    });
    
  } catch (error) {
    console.error('❌ Error crítico en el servicio:', error);
    process.exit(1);
  }
}

// Manejo de señales para cierre limpio
process.on('SIGINT', async () => {
  console.log('🛑 Cerrando servicio de carrito de compras...');
  try {
    await consumer.disconnect();
    await redis.quit();
    console.log('✅ Servicios cerrados correctamente');
  } catch (error) {
    console.error('❌ Error cerrando servicios:', error);
  }
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('🛑 SIGTERM recibido, cerrando servicio...');
  try {
    await consumer.disconnect();
    await redis.quit();
    console.log('✅ Servicios cerrados correctamente');
  } catch (error) {
    console.error('❌ Error cerrando servicios:', error);
  }
  process.exit(0);
});

// Iniciar el servicio
console.log('🚀 Iniciando servicio de procesamiento de carrito...');
console.log('📋 Configuración:');
console.log(`   - Kafka: localhost:9092`);
console.log(`   - Topic: email-cart-requests`);
console.log(`   - Redis: redis:6379`);
console.log('=======================================');

iniciarServicio().catch((error) => {
  console.error('💥 Fallo crítico al iniciar el servicio:', error);
  process.exit(1);
});
