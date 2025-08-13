const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');

// MongoDB Atlas connection
const mongoUri = 'mongodb+srv://brayam0055:Q5hB6MW2iUpXaVd@cluster0.rvrn2ng.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0';
const mongoClient = new MongoClient(mongoUri);

// Kafka configuration
const kafka = new Kafka({
  clientId: 'cart-consumer-service',
  brokers: ['localhost:9092'], // Mismo puerto que tu Docker
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

const consumer = kafka.consumer({ 
  groupId: 'shopping-cart-mongo-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000
});

async function iniciarServicio() {
  try {
    // Conectar a MongoDB Atlas
    await mongoClient.connect();
    console.log('✅ Conectado a MongoDB Atlas');
    
    // Seleccionar database y collection
    const db = mongoClient.db('shopping_cart_db'); // Puedes cambiar el nombre
    const collection = db.collection('cart_emails'); // Collection para los emails de carrito
    
    // Conectar consumer de Kafka
    await consumer.connect();
    console.log('✅ Conectado a Kafka');
    
    // Suscribirse al topic exacto
    await consumer.subscribe({ 
      topic: 'email-cart-requests', // Topic exacto de tu aplicación C#
      fromBeginning: false // Solo mensajes nuevos
    });
    
    console.log('🔄 Esperando mensajes del carrito...');
    
    // Procesar mensajes
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          // Parsear el mensaje de Kafka
          const cartData = JSON.parse(message.value.toString());
          const cartHeaderId = message.key.toString();
          
          console.log(`📦 Mensaje recibido - Topic: ${topic}, Partition: ${partition}, Offset: ${message.offset}`);
          console.log(`🛒 Cart ID: ${cartHeaderId}`);
          
          // Preparar documento para MongoDB con toda la información
          const emailDocument = {
            // Información del carrito
            cartHeaderId: cartData.CartHeader.CartHeaderId,
            userId: cartData.CartHeader.UserId,
            couponCode: cartData.CartHeader.CouponCode || null,
            discount: cartData.CartHeader.Discount || 0,
            cartTotal: cartData.CartHeader.CartTotal,
            
            // Detalles de productos en el carrito
            cartItems: cartData.CartDetailsDtos.map(item => ({
              productId: item.ProductId,
              count: item.Count,
              cartHeaderId: item.CartHeaderId,
              productInfo: item.ProductDto ? {
                name: item.ProductDto.Name,
                price: item.ProductDto.Price,
                description: item.ProductDto.Description,
                categoryName: item.ProductDto.CategoryName,
                imageUrl: item.ProductDto.ImageUrl
              } : null
            })),
            
            // Metadatos de Kafka y procesamiento
            _kafka_metadata: {
              topic,
              partition,
              offset: message.offset,
              key: cartHeaderId,
              timestamp: message.timestamp,
              processed_at: new Date()
            },
            
            // Estado del email
            email_status: {
              status: 'pending', // pending, sent, failed
              created_at: new Date(),
              attempts: 0
            }
          };
          
          // Insertar en MongoDB Atlas
          const result = await collection.insertOne(emailDocument);
          
          console.log(`✅ Carrito guardado en MongoDB:`);
          console.log(`   - MongoDB ID: ${result.insertedId}`);
          console.log(`   - Usuario: ${cartData.CartHeader.UserId}`);
          console.log(`   - Total: $${cartData.CartHeader.CartTotal}`);
          console.log(`   - Items: ${cartData.CartDetailsDtos.length} productos`);
          console.log(`   - Kafka Offset: ${message.offset}`);
          console.log('-----------------------------------');
          
        } catch (error) {
          console.error('❌ Error procesando mensaje del carrito:', error);
          console.error('Mensaje original:', message.value.toString());
          
          // Opcional: Guardar errores en una collection separada
          try {
            const errorCollection = db.collection('processing_errors');
            await errorCollection.insertOne({
              error: error.message,
              original_message: message.value.toString(),
              kafka_metadata: {
                topic,
                partition,
                offset: message.offset,
                timestamp: message.timestamp
              },
              error_timestamp: new Date()
            });
          } catch (logError) {
            console.error('❌ Error guardando log de error:', logError);
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
    await mongoClient.close();
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
    await mongoClient.close();
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
console.log(`   - MongoDB: Cluster0`);
console.log('=======================================');

iniciarServicio().catch((error) => {
  console.error('💥 Fallo crítico al iniciar el servicio:', error);
  process.exit(1);
});