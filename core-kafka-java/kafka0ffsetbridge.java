import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;

public class Main {

    // ==========================================
    // SPEC-FIRST CONTRACT
    // ==========================================

    enum EventType {
        TED,
        BOLETO,
        DEBITO_AUTOMATICO
    }

    static class PaymentEvent {
        String eventId;
        EventType eventType;
        String transactionId;
        BigDecimal amount;
        String currency;
        String payerDocument;
        String payerAccount;
        String receiverDocument;
        String receiverAccount;
        String barcode;
        String agreementId;
        LocalDate dueDate;
        LocalDate scheduledDate;

        PaymentEvent(
                String eventId,
                EventType eventType,
                String transactionId,
                BigDecimal amount,
                String currency,
                String payerDocument,
                String payerAccount,
                String receiverDocument,
                String receiverAccount,
                String barcode,
                String agreementId,
                LocalDate dueDate,
                LocalDate scheduledDate
        ) {
            this.eventId = eventId;
            this.eventType = eventType;
            this.transactionId = transactionId;
            this.amount = amount;
            this.currency = currency;
            this.payerDocument = payerDocument;
            this.payerAccount = payerAccount;
            this.receiverDocument = receiverDocument;
            this.receiverAccount = receiverAccount;
            this.barcode = barcode;
            this.agreementId = agreementId;
            this.dueDate = dueDate;
            this.scheduledDate = scheduledDate;
        }
    }

    // ==========================================
    // SPEC-FIRST VALIDATOR
    // ==========================================

    static class ContractValidator {

        static List<String> validate(PaymentEvent event) {

            List<String> errors = new ArrayList<>();

            if (event == null) {
                errors.add("Evento nulo");
                return errors;
            }

            if (isBlank(event.eventId)) {
                errors.add("eventId obrigatorio");
            }

            if (event.eventType == null) {
                errors.add("eventType obrigatorio");
                return errors;
            }

            if (isBlank(event.transactionId)) {
                errors.add("transactionId obrigatorio");
            }

            if (event.amount == null) {
                errors.add("amount obrigatorio");
            } else if (event.amount.compareTo(BigDecimal.ZERO) <= 0) {
                errors.add("amount deve ser maior que zero");
            }

            if (!"BRL".equals(event.currency)) {
                errors.add("currency deve ser BRL");
            }

            if (isBlank(event.payerDocument)) {
                errors.add("payerDocument obrigatorio");
            }

            if (isBlank(event.payerAccount)) {
                errors.add("payerAccount obrigatorio");
            }

            switch (event.eventType) {

                case TED:
                    validateTed(event, errors);
                    break;

                case BOLETO:
                    validateBoleto(event, errors);
                    break;

                case DEBITO_AUTOMATICO:
                    validateDebitoAutomatico(event, errors);
                    break;

                default:
                    errors.add("Tipo de evento nao suportado");
            }

            return errors;
        }

        private static void validateTed(
                PaymentEvent event,
                List<String> errors
        ) {
            if (isBlank(event.receiverDocument)) {
                errors.add("TED: receiverDocument obrigatorio");
            }

            if (isBlank(event.receiverAccount)) {
                errors.add("TED: receiverAccount obrigatorio");
            }
        }

        private static void validateBoleto(
                PaymentEvent event,
                List<String> errors
        ) {
            if (isBlank(event.barcode)) {
                errors.add("BOLETO: barcode obrigatorio");
            } else if (!event.barcode.matches("[0-9]{44,48}")) {
                errors.add("BOLETO: barcode deve conter de 44 a 48 digitos");
            }

            if (event.dueDate == null) {
                errors.add("BOLETO: dueDate obrigatorio");
            }
        }

        private static void validateDebitoAutomatico(
                PaymentEvent event,
                List<String> errors
        ) {
            if (isBlank(event.agreementId)) {
                errors.add("DEBITO AUTOMATICO: agreementId obrigatorio");
            }

            if (event.scheduledDate == null) {
                errors.add("DEBITO AUTOMATICO: scheduledDate obrigatorio");
            }
        }

        private static boolean isBlank(String value) {
            return value == null || value.trim().isEmpty();
        }
    }

    // ==========================================
    // KAFKA SIMULATOR
    // ==========================================

    static class KafkaRecord {

        final int partition;
        final long offset;
        final PaymentEvent event;

        KafkaRecord(int partition, long offset, PaymentEvent event) {
            this.partition = partition;
            this.offset = offset;
            this.event = event;
        }
    }

    static class KafkaTopic {

        private final String topicName;
        private final List<KafkaRecord> records = new ArrayList<>();

        KafkaTopic(String topicName) {
            this.topicName = topicName;
        }

        void publish(PaymentEvent event) {

            long offset = records.size();

            KafkaRecord record = new KafkaRecord(
                    0,
                    offset,
                    event
            );

            records.add(record);

            System.out.println(
                    "[KAFKA PRODUCER] Topic=" + topicName
                    + " | Partition=0"
                    + " | Offset=" + offset
                    + " | Event=" + event.eventType
            );
        }

        List<KafkaRecord> getRecords() {
            return records;
        }
    }

    // ==========================================
    // CONSUMER GROUP
    // ==========================================

    static class ConsumerGroup {

        private final String groupId;
        private long committedOffset = 0;

        private final Set<String> processedEvents = new HashSet<>();

        ConsumerGroup(String groupId) {
            this.groupId = groupId;
        }

        void consume(KafkaTopic topic) {

            System.out.println("\n====================================");
            System.out.println("CONSUMER GROUP: " + groupId);
            System.out.println("OFFSET INICIAL: " + committedOffset);
            System.out.println("====================================");

            List<KafkaRecord> records = topic.getRecords();

            while (committedOffset < records.size()) {

                KafkaRecord record = records.get((int) committedOffset);

                System.out.println(
                        "\n[CONSUMER] Group=" + groupId
                        + " | Offset=" + record.offset
                );

                PaymentEvent event = record.event;

                if (event == null) {
                    System.out.println("[REJECTED] Evento nulo");
                    commit(record);
                    continue;
                }

                if (processedEvents.contains(event.eventId)) {
                    System.out.println(
                            "[DUPLICATE] Evento ja processado: "
                            + event.eventId
                    );

                    commit(record);
                    continue;
                }

                List<String> errors =
                        ContractValidator.validate(event);

                if (!errors.isEmpty()) {

                    System.out.println(
                            "[CONTRACT REJECTED] "
                            + event.eventId
                    );

                    for (String error : errors) {
                        System.out.println(" - " + error);
                    }

                    commit(record);
                    continue;
                }

                processEvent(event);

                processedEvents.add(event.eventId);

                commit(record);
            }

            System.out.println(
                    "\n[CONSUMER GROUP FINALIZADO] "
                    + groupId
                    + " | Committed Offset="
                    + committedOffset
            );
        }

        private void processEvent(PaymentEvent event) {

            if ("cg-pagamentos-real".equals(groupId)) {

                System.out.println(
                        "[PAGAMENTOS REAL] Processando pagamento"
                );

                System.out.println(
                        "Tipo: " + event.eventType
                        + " | Transacao: " + event.transactionId
                        + " | Valor: R$ " + event.amount
                );

            } else if ("cg-datamesh-regulatorio".equals(groupId)) {

                System.out.println(
                        "[DATAMESH REGULATORIO] Registrando evento"
                );

                System.out.println(
                        "Auditoria: " + event.eventId
                        + " | Tipo: " + event.eventType
                        + " | Valor: R$ " + event.amount
                        + " | Moeda: " + event.currency
                );
            }
        }

        private void commit(KafkaRecord record) {

            committedOffset = record.offset + 1;

            System.out.println(
                    "[OFFSET COMMIT]"
                    + " | Group=" + groupId
                    + " | Partition=" + record.partition
                    + " | Next Offset=" + committedOffset
            );
        }

        long getCommittedOffset() {
            return committedOffset;
        }
    }

    // ==========================================
    // EVENT FACTORY
    // ==========================================

    static PaymentEvent createTed() {

        return new PaymentEvent(
                "EVT-TED-001",
                EventType.TED,
                "TX-TED-001",
                new BigDecimal("1500.00"),
                "BRL",
                "12345678901",
                "0001-12345",
                "98765432100",
                "0002-98765",
                null,
                null,
                null,
                null
        );
    }

    static PaymentEvent createBoleto() {

        return new PaymentEvent(
                "EVT-BOLETO-001",
                EventType.BOLETO,
                "TX-BOLETO-001",
                new BigDecimal("350.90"),
                "BRL",
                "12345678901",
                "0001-12345",
                null,
                null,
                "12345678901234567890123456789012345678901234",
                null,
                LocalDate.of(2026, 12, 10),
                null
        );
    }

    static PaymentEvent createDebitoAutomatico() {

        return new PaymentEvent(
                "EVT-DEBITO-001",
                EventType.DEBITO_AUTOMATICO,
                "TX-DEBITO-001",
                new BigDecimal("89.90"),
                "BRL",
                "12345678901",
                "0001-12345",
                null,
                null,
                null,
                "AGREEMENT-001",
                null,
                LocalDate.of(2026, 12, 15)
        );
    }

    static PaymentEvent createInvalidEvent() {

        return new PaymentEvent(
                "EVT-INVALID-001",
                EventType.TED,
                "TX-INVALID-001",
                new BigDecimal("-100.00"),
                "BRL",
                "12345678901",
                "0001-12345",
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    // ==========================================
    // MAIN EXECUTION
    // ==========================================

    public static void main(String[] args) {

        System.out.println("====================================");
        System.out.println(" KAFKA OFFSET BRIDGE");
        System.out.println(" SPEC-FIRST PAYMENT EVENT VALIDATION");
        System.out.println("====================================");

        KafkaTopic topic = new KafkaTopic(
                "payment-events"
        );

        // PRODUCER: publicando eventos

        System.out.println("\n--- PRODUCING EVENTS ---");

        topic.publish(createTed());
        topic.publish(createBoleto());
        topic.publish(createDebitoAutomatico());
        topic.publish(createInvalidEvent());

        // CONSUMER GROUP 1

        ConsumerGroup pagamentosReal =
                new ConsumerGroup("cg-pagamentos-real");

        // CONSUMER GROUP 2

        ConsumerGroup datameshRegulatorio =
                new ConsumerGroup("cg-datamesh-regulatorio");

        // PROCESSAMENTO INDEPENDENTE

        pagamentosReal.consume(topic);

        datameshRegulatorio.consume(topic);

        // ======================================
        // OFFSET REPORT
        // ======================================

        System.out.println("\n====================================");
        System.out.println(" FINAL OFFSET REPORT");
        System.out.println("====================================");

        System.out.println(
                "cg-pagamentos-real: "
                + pagamentosReal.getCommittedOffset()
        );

        System.out.println(
                "cg-datamesh-regulatorio: "
                + datameshRegulatorio.getCommittedOffset()
        );

        System.out.println("\n====================================");
        System.out.println(" EXECUTION COMPLETED");
        System.out.println("====================================");
    }
}
