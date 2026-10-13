#include <iostream>
#include <vector>
#include <string>
#include <string_view>
#include <unordered_map>
#include <functional>
#include <mutex>
#include <thread>
#include <chrono>

// Represents a generic telemetry message payload
struct TelemetryMessage {
    std::string topic;
    double value;
    std::string metadata;
};

// Thread-Safe Publish-Subscribe Broker
class EventBroker {
private:
    // Callback function type for subscribers
    using Callback = std::function<void(const TelemetryMessage&)>;

    // Map of Topic Name -> List of Subscriber Callbacks
    std::unordered_map<std::string, std::vector<Callback>> m_subscribers;
    mutable std::mutex m_broker_mutex;

public:
    EventBroker() = default;

    // Subscribes a callback function to a specific topic
    void subscribe(std::string_view topic, Callback callback) {
        std::lock_guard<std::mutex> lock(m_broker_mutex);
        m_subscribers[std::string(topic)].push_back(callback);
        std::cout << "[Broker] Registered new subscriber for topic: " << topic << "\n";
    }

    // Publishes a message to all subscribers of a topic asynchronously
    void publish(const TelemetryMessage& message) {
        std::vector<Callback> callbacks_to_notify;

        // Scope the lock tightly: copy callbacks to notify to avoid blocking the broker 
        // while user-defined callbacks are executing (preventing deadlocks)
        {
            std::lock_guard<std::mutex> lock(m_broker_mutex);
            auto it = m_subscribers.find(message.topic);
            if (it != m_subscribers.end()) {
                callbacks_to_notify = it->second;
            }
        }

        // Execute callbacks outside the mutex lock
        for (const auto& callback : callbacks_to_notify) {
            callback(message);
        }
    }
};

// Simulated Sensor Node (Publisher)
void sensorPublisherNode(EventBroker& broker) {
    double mock_voltage = 12.0;
    for (int i = 1; i <= 3; ++i) {
        mock_voltage -= 0.1;
        TelemetryMessage msg{"battery_voltage", mock_voltage, "mV"};
        
        std::cout << "[Battery Node] Publishing battery state: " << mock_voltage << "V\n";
        broker.publish(msg);
        
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }
}

// Simulated Actuator Safety Node (Subscriber)
void safetyControllerNode(const TelemetryMessage& msg) {
    std::cout << "  [Safety Node] Received data on topic [" << msg.topic 
              << "] -> Value: " << msg.value << msg.metadata << "\n";
    
    if (msg.value < 11.8) {
        std::cout << "  ⚠️  [Safety Node] WARNING: Low battery detected! Triggering low-power safe state.\n";
    }
}

// Simulated Cloud Diagnostics Logger (Subscriber)
void diagnosticsLoggerNode(const TelemetryMessage& msg) {
    std::cout << "  [Diagnostics Node] Logging: [" << msg.topic << "] = " << msg.value << "\n";
}

int main() {
    std::cout << "=== 🤖 Real-Time Robot Event Broker Started 🤖 ===\n\n";

    EventBroker central_broker;

    // 1. Initialize Subscribers
    // The safety node only cares about battery voltage
    central_broker.subscribe("battery_voltage", safetyControllerNode);
    // The diagnostics node also cares about battery voltage
    central_broker.subscribe("battery_voltage", diagnosticsLoggerNode);

    std::cout << "\n--- Starting Asynchronous Node Execution ---\n\n";

    // 2. Start Publisher Node on an independent thread
    std::thread publisher_thread(sensorPublisherNode, std::ref(central_broker));

    // Wait for execution to complete
    publisher_thread.join();

    std::cout << "\n=== 🛑 Event Broker System Shutting Down 🛑 ===\n";
    return 0;
}
