# Entrega – Endpoints y Guarantee Event Consume Order

| Entregable | Archivo |
|---|---|
| 1. Pruebas de endpoints (capturas y explicaciones) | `Parte1_Endpoints.pdf` |
| 2. Cambios para garantizar el orden de consumo | `Cambios_Orden_Eventos.pdf` |
| 3. Prueba de reproducción de eventos | `Prueba_Replay_Eventos.pdf` |

Material de apoyo: `postman_collection.json` (colección con variables), `capturas_endpoints/`, `evidencia_replay/`, `cambios.diff`, `parte1_endpoints.txt`.

Infraestructura: `docker compose up -d` (Kafka, MongoDB, MySQL). Build con JDK 17. Command API en el puerto 5000 (en las pruebas se usó 5002 porque macOS ocupa el 5000), Query API en 5001.
