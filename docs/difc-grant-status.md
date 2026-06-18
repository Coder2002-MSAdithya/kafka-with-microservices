# DIFC grant and data-flow status — kafka-with-microservices

Generated from workflow logs under `/tmp/jug-kafka-demo/logs`.

## kafka-with-microservices
Logs: `/tmp/jug-kafka-demo/logs` (present)

### Data flow
| Metric | Value |
|--------|-------|
| orders/users posted | ? |
| validated events | 0 |
| failed events | 0 |
| aggregation decisions | 0 |

### Capability grants (grantor decision)
| Grantor | Requester | Tag | CAN_ADD | CAN_REMOVE | Notes |
|--------|-----------|-----|---------|------------|-------|
| PaymentService | order-svc | billing | GRANTED | - |  |
| StockService | order-svc | stock | GRANTED | - |  |
| StockService | validation-svc | stock | GRANTED | DENIED | output relation STOCK_CHECK_EVENT_TOPIC → VALIDATION_EVENT_TOPIC Sink( |
| ValidationService | order-svc | validation | GRANTED | - |  |
| ValidationService | payment-svc | validation | GRANTED | DENIED | output relation VALIDATION_EVENT_TOPIC → BILLING_EVENT_TOPIC Sink(BILL |

### External connections
| Principal | Target | Allowed | Expected? |
|-----------|--------|---------|-----------|
| payment-svc | - | - | OK (broker-only) |
| stock-svc | - | - | OK (broker-only) |
| validation-svc | - | - | OK (broker-only) |
