#include <iostream>
#include <sstream>
#include <iomanip>
#include <avro/Specific.hh>
#include <avro/Compiler.hh>
#include <avro/Generic.hh>
#include <avro/Reader.hh>
#include <avro/Writer.hh>
#include "../SES_NWSLA_Collector/src/nwsla_coll_defs.h"

enum DatumType {
    DATUM_IP_TYPE,
    DATUM_PROT_TYPE,
    DATUM_SESSION_STATE,
    DATUM_OTHER
};

const std::string schema_json_result = R"({
  "type": "record",
  "name": "latencyresult",
  "fields": [
    {
      "name": "agent_ip",
      "type": "string"
    },
    {
      "name": "state",
      "type": {
        "type": "enum",
        "name": "SessionState",
        "symbols": ["SESSION_STATE_RUNNING", "SESSION_STATE_STOPPING", "SESSION_STATE_STOPPED"]
      }
    },
    {
      "name": "timestamp",
      "type": "int"
    },
    {
      "name": "packets_sent",
      "type": "int"
    },
    {
      "name": "packets_received",
      "type": "int"
    },
    {
      "name": "packet_loss",
      "type": "int"
    },
    {
      "name": "total_burst_time",
      "type": "double"
    },
    {
      "name": "rtt_min",
      "type": "int"
    },
    {
      "name": "rtt_avg",
      "type": "int"
    },
    {
      "name": "rtt_max",
      "type": "int"
    },
    {
      "name": "rtt_mdev",
      "type": "int"
    }
  ]
})";

const std::string schema_json_client = R"({
  "type": "record",
  "name": "clientparams",
  "fields": [
    {
      "name": "agent_ip",
      "type": "string"
    },
    {
      "name": "dest_ip",
      "type": "string"
    },
    {
      "name": "ipType",
      "type": {
        "type": "enum",
        "name": "IpType",
        "symbols": ["IPTYPE_IPV4", "IPTYPE_IPV6", "IPTYPE_INVALID"]
      }
    },
    {
      "name": "protType",
      "type": {
        "type": "enum",
        "name": "ProtType",
        "symbols": ["PROTTYPE_ICMP", "PROTTYPE_TCP", "PROTTYPE_UNSUPPORTED"]
      }
    },
    {
      "name": "interval",
      "type": "int"
    },
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "state",
      "type": {
        "type": "enum",
        "name": "SessionState",
        "symbols": ["SESSION_STATE_RUNNING", "SESSION_STATE_STOPPING", "SESSION_STATE_STOPPED"]
      }
    },
    {
      "name": "port",
      "type": "int"
    }
  ]
})";


inline std::string to_string(IpType ipType) {
    switch (ipType) {
        case IpType::IPTYPE_IPV4: return "IPTYPE_IPV4";
        case IpType::IPTYPE_IPV6: return "IPTYPE_IPV6";
        case IpType::IPTYPE_INVALID: return "IPTYPE_INVALID";
        default: throw std::runtime_error("Unknown IpType");
    }
}

inline std::string to_string(ProtType protType) {
    switch (protType) {
        case ProtType::PROTTYPE_ICMP: return "PROTTYPE_ICMP";
        case ProtType::PROTTYPE_TCP: return "PROTTYPE_TCP";
        case ProtType::PROTTYPE_UNSUPPORTED: return "PROTTYPE_UNSUPPORTED";
        default: throw std::runtime_error("Unknown ProtType");
    }
}

inline std::string to_string(SessionState state) {
    switch (state) {
        case SessionState::SESSION_STATE_RUNNING: return "SESSION_STATE_RUNNING";
        case SessionState::SESSION_STATE_STOPPING: return "SESSION_STATE_STOPPING";
        case SessionState::SESSION_STATE_STOPPED: return "SESSION_STATE_STOPPED";
        default: throw std::runtime_error("Unknown SessionState");
    }
}

std::string datumToString(const avro::GenericDatum& datum, DatumType datumType) {
    if (datum.type() == avro::AVRO_INT) {
        int intValue = datum.value<int>();
        switch (datumType) {
            case DATUM_IP_TYPE:
                switch (intValue) {
                    case IPTYPE_IPV4: return "IPTYPE_IPV4";
                    case IPTYPE_IPV6: return "IPTYPE_IPV6";
                    case IPTYPE_INVALID: return "IPTYPE_INVALID";
                }
                break;
            case DATUM_PROT_TYPE:
                switch (intValue) {
                    case PROTTYPE_ICMP: return "PROTTYPE_ICMP";
                    case PROTTYPE_TCP: return "PROTTYPE_TCP";
                    case PROTTYPE_UNSUPPORTED: return "PROTTYPE_UNSUPPORTED";
                }
                break;
            case DATUM_SESSION_STATE:
                switch (intValue) {
                    case SESSION_STATE_RUNNING: return "SESSION_STATE_RUNNING";
                    case SESSION_STATE_STOPPING: return "SESSION_STATE_STOPPING";
                    case SESSION_STATE_STOPPED: return "SESSION_STATE_STOPPED";
                }
                break;
            case DATUM_OTHER:
                return std::to_string(intValue);
        }
    }
    return "";
}


clientparams cp = {
  "192.168.1.1", 
  "192.168.1.2", 
  IPTYPE_IPV4,
  PROTTYPE_TCP, 
  1000, 
  1, 
  SESSION_STATE_RUNNING , 
  8080
};

latencyresult lr = {
  "192.168.1.1",
  SESSION_STATE_RUNNING,
  3,
  4,
  5,
  6,
  7.6,
  8,
  9,
  10,
  11
};

std::vector<uint8_t> NwSlaCResultSerializeToAvro(struct latencyresult r, const std::string structSchema){
    std::istringstream schema_stream(structSchema);
    avro::ValidSchema schema;
    avro::compileJsonSchema(schema_stream, schema);

    avro::GenericDatum datum(schema);
    avro::GenericRecord& record = datum.value<avro::GenericRecord>();

    record.setFieldAt(0, avro::GenericDatum(r.agent_ip));
    record.setFieldAt(1, avro::GenericDatum(to_string(r.state)));
    record.setFieldAt(2, avro::GenericDatum(static_cast<int>(r.timestamp)));  // Use the string representation
    record.setFieldAt(3, avro::GenericDatum(static_cast<int>(r.packets_sent)));
    record.setFieldAt(4, avro::GenericDatum(static_cast<int>(r.packets_received)));
    record.setFieldAt(5, avro::GenericDatum(static_cast<int>(r.packet_loss)));
    record.setFieldAt(6, avro::GenericDatum(static_cast<int>(r.total_burst_time)));
    record.setFieldAt(7, avro::GenericDatum(static_cast<int>(r.rtt_min)));
    record.setFieldAt(8, avro::GenericDatum(static_cast<int>(r.rtt_avg)));
    record.setFieldAt(9, avro::GenericDatum(static_cast<int>(r.rtt_max)));
    record.setFieldAt(10, avro::GenericDatum(static_cast<int>(r.rtt_mdev)));

    std::unique_ptr<avro::OutputStream> output_stream = avro::memoryOutputStream();
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*output_stream);

    avro::encode(*encoder, datum);

    std::unique_ptr<avro::InputStream> input_stream = avro::memoryInputStream(*output_stream);
    std::vector<uint8_t> serialized_data;
    const uint8_t* data;
    size_t len;

    while (input_stream->next(&data, &len)) {
        serialized_data.insert(serialized_data.end(), data, data + len);
    }

    std::cout << "Serialized data size: " << serialized_data.size() << " bytes" << std::endl;

    return serialized_data;
}

std::vector<uint8_t> NwSlaClientSerializeToAvro(const clientparams& c, const std::string& structSchema) {
    std::istringstream schema_stream(structSchema);
    avro::ValidSchema schema;
    avro::compileJsonSchema(schema_stream, schema);

    avro::GenericDatum datum(schema);
    avro::GenericRecord& record = datum.value<avro::GenericRecord>();

    record.setFieldAt(0, avro::GenericDatum(c.agent_ip));
    record.setFieldAt(1, avro::GenericDatum(c.dest_ip));
    record.setFieldAt(2, avro::GenericDatum(static_cast<int>(c.ipType)));  // Use the integer value
    record.setFieldAt(3, avro::GenericDatum(static_cast<int>(c.protType)));  // Use the integer value
    record.setFieldAt(4, avro::GenericDatum(static_cast<int>(c.interval)));
    record.setFieldAt(5, avro::GenericDatum(static_cast<int>(c.id)));
    record.setFieldAt(6, avro::GenericDatum(static_cast<int>(c.state)));  // Use the integer value
    record.setFieldAt(7, avro::GenericDatum(static_cast<int>(c.port)));

    std::unique_ptr<avro::OutputStream> output_stream = avro::memoryOutputStream();
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*output_stream);

    avro::encode(*encoder, datum);

    std::vector<uint8_t> serialized_data;
    std::unique_ptr<avro::InputStream> input_stream = avro::memoryInputStream(*output_stream);
    const uint8_t* data;
    size_t len;

    while (input_stream->next(&data, &len)) {
        serialized_data.insert(serialized_data.end(), data, data + len);
    }

    return serialized_data;
}


const std::vector<uint8_t> NwSlaSerializeToAvro(){
  std::vector<uint8_t> client_serial = NwSlaClientSerializeToAvro(cp, schema_json_client);
  std::vector<uint8_t> result_serial = NwSlaCResultSerializeToAvro(lr, schema_json_result);

  std::vector<uint8_t> combined_result;
  combined_result.reserve(client_serial.size() + result_serial.size());
  combined_result.insert(combined_result.end(), client_serial.begin(), client_serial.end());
  combined_result.insert(combined_result.end(), result_serial.begin(), result_serial.end());

  for (const auto& byte : combined_result) {
        std::cout << static_cast<int>(byte) << " ";
    }
  std::cout << std::endl;

  return combined_result;

}

void NwSlaClientDeserializeFromAvro(const std::vector<uint8_t>& serialized_data, const std::string& structSchema) {
    try {
        std::istringstream schema_stream(structSchema);
        avro::ValidSchema schema;
        avro::compileJsonSchema(schema_stream, schema);

        std::unique_ptr<avro::InputStream> input_stream = avro::memoryInputStream(serialized_data.data(), serialized_data.size());
        avro::DecoderPtr decoder = avro::binaryDecoder();
        decoder->init(*input_stream);

        avro::GenericDatum datum(schema);
        avro::decode(*decoder, datum);

        avro::GenericRecord& record = datum.value<avro::GenericRecord>();

        std::cout << "agent_ip: " << record.fieldAt(0).value<std::string>() << std::endl;
        std::cout << "dest_ip: " << record.fieldAt(1).value<std::string>() << std::endl;
        std::cout << "ipType: " << (record.fieldAt(2), DATUM_IP_TYPE) << std::endl;
        std::cout << "protType: " << datumToString(record.fieldAt(3), DATUM_PROT_TYPE) << std::endl;
        std::cout << "interval: " << record.fieldAt(4).value<int>() << std::endl;
        std::cout << "id: " << record.fieldAt(5).value<int>() << std::endl;
        std::cout << "state: " << datumToString(record.fieldAt(6), DATUM_SESSION_STATE) << std::endl;
        std::cout << "port: " << record.fieldAt(7).value<int>() << std::endl;
    } catch (const avro::Exception& e) {
        std::cerr << "Avro exception: " << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Standard exception: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknown exception occurred" << std::endl;
    }
}
