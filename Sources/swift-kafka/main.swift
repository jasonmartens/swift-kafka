import SwiftKafka
import AsyncHTTPClient

let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)
defer {
    try? httpClient.syncShutdown()
}

func submitRecord(record: KafkaConsumerRecord, c: HTTPClient) -> Void {
    do {
        var r = try HTTPClient.Request(url: "http://localhost:80/post", method: .POST)
        r.body = .string(record.value!)
        
        c.execute(request: r).whenComplete { result in
            switch result {
            case .failure(let error):
                print("Failed to submit request: \(error)")
            case .success(let response):
                if response.status == .ok {
                    print(response.status)
                } else {
                    print("got response: \(response.status)")
                }
            }
        }
    } catch {
        print("Unexpected error: \(error).")
    }
}

do {
    let producer = try KafkaProducer()
    guard producer.connect(brokers: "localhost:9092") == 1 else {
        throw KafkaError(rawValue: 8)
    }
    producer.send(producerRecord: KafkaProducerRecord(topic: "test", value: "Hello world", key: "Key")) { result in
        switch result {
        case .success(let message):
            print("Message at offset \(message.offset) successfully sent")
        case .failure(let error):
            print("Error producing: \(error)")
        }
    }
} catch {
    print("Error creating producer: \(error)")
}

do {
    let config = KafkaConfig()
    config.groupId = "Kitura"
    config.autoOffsetReset = .beginning
    let consumer = try KafkaConsumer(config: config)
    guard consumer.connect(brokers: "localhost:9092") == 1 else {
        throw KafkaError(rawValue: 8)
    }
    try consumer.subscribe(topics: ["test"])
    while(true) {
        let records = try consumer.poll()
        if (records.count > 0) {
            print(records)
            records.forEach { r in
                submitRecord(record: r, c: httpClient)
            }
        } else {
            print("No records")
        }
    }
} catch {
    print("Error creating consumer: \(error)")
}
