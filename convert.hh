#include <iostream>
#include <sstream>
#include <iomanip>
#include <avro/Specific.hh>
#include <avro/Compiler.hh>
#include <avro/Generic.hh>
#include <avro/Reader.hh>
#include <avro/Writer.hh>
#include "../SES_NWSLA_Collector/src/nwsla_coll_defs.h"

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
        "symbols": ["SESSION_STATE_RUNNING", "SESSION_STATE_STOPPING", "SESSION_STATE_STOPPED", "FILLER"]
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
        "symbols": ["IPTYPE_IPV4", "IPTYPE_IPV6", "IPTYPE_INVALID", "FILLER"]
      }
    },
    {
      "name": "protType",
      "type": {
        "type": "enum",
        "name": "ProtType",
        "symbols": ["PROTTYPE_ICMP", "PROTTYPE_TCP", "PROTTYPE_UNSUPPORTED", "FILLER"]
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
        "symbols": ["SESSION_STATE_RUNNING", "SESSION_STATE_STOPPING", "SESSION_STATE_STOPPED", "FILLER"]
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

clientparams cp = {
  "100.000.000", 
  "999.999.999", 
  IPTYPE_IPV4,
  PROTTYPE_TCP, 
  7991, 
  7, 
  SESSION_STATE_STOPPED, 
  8900
};

latencyresult lr = {
  "100.000.000",
  SESSION_STATE_STOPPING,
  99999999,
  69,
  69,
  0,
  2.25,
  1,
  5,
  10,
  7
};

std::vector<uint8_t> NwSlaResultSerializeToAvro(const latencyresult& result, const std::string& structSchema) {
    std::istringstream schema_stream(structSchema);
    avro::ValidSchema schema;
    avro::compileJsonSchema(schema_stream, schema);

    avro::GenericDatum datum(schema);
    avro::GenericRecord& record = datum.value<avro::GenericRecord>();

    record.setFieldAt(0, avro::GenericDatum(result.agent_ip));
    record.setFieldAt(1, avro::GenericDatum(static_cast<int>(result.state)));  // Use the integer value for enum
    record.setFieldAt(2, avro::GenericDatum(static_cast<int>(result.timestamp)));
    record.setFieldAt(3, avro::GenericDatum(static_cast<int>(result.packets_sent)));
    record.setFieldAt(4, avro::GenericDatum(static_cast<int>(result.packets_received)));
    record.setFieldAt(5, avro::GenericDatum(static_cast<int>(result.packet_loss)));
    record.setFieldAt(6, avro::GenericDatum(static_cast<double>(result.total_burst_time)));
    record.setFieldAt(7, avro::GenericDatum(static_cast<int>(result.rtt_min)));
    record.setFieldAt(8, avro::GenericDatum(static_cast<int>(result.rtt_avg)));
    record.setFieldAt(9, avro::GenericDatum(static_cast<int>(result.rtt_max)));
    record.setFieldAt(10, avro::GenericDatum(static_cast<int>(result.rtt_mdev)));

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


void NwSlaResultDeserializeFromAvro(const std::vector<uint8_t>& serialized_data, const std::string& structSchema) {
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

        std::cout << "Latency Results:\n";

        std::cout << "agent_ip: " << record.fieldAt(0).value<std::string>() << std::endl;

        avro::GenericEnum stateEnum = record.fieldAt(1).value<avro::GenericEnum>();
        int stateIndex = stateEnum.value(); // Get the enum index
        SessionState stateValue = static_cast<SessionState>(stateIndex);
        std::cout << "state: " << to_string(stateValue) << std::endl;

        //avro::GenericEnum protTypeEnum = record.fieldAt(1).value<avro::GenericEnum>();
        //int protTypeIndex = protTypeEnum.value(); // Get the enum index
        //ProtType protTypeValue = static_cast<ProtType>(protTypeIndex);
        //std::cout << "protType: " << to_string(protTypeValue) << std::endl;

        std::cout << "timestamp: " << record.fieldAt(2).value<int>() << std::endl;
        std::cout << "packets_sent: " << record.fieldAt(3).value<int>() << std::endl;
        std::cout << "packets_received: " << record.fieldAt(4).value<int>() << std::endl;
        std::cout << "packet_loss: " << record.fieldAt(5).value<int>() << std::endl;
        std::cout << "total_burst_time: " << record.fieldAt(6).value<double>() << std::endl;
        std::cout << "rtt_min: " << record.fieldAt(7).value<int>() << std::endl;
        std::cout << "rtt_avg: " << record.fieldAt(8).value<int>() << std::endl;
        std::cout << "rtt_max: " << record.fieldAt(9).value<int>() << std::endl;
        std::cout << "rtt_mdev: " << record.fieldAt(10).value<int>() << std::endl;
       // std::cout << "k: " << record.fieldAt(10).value<int>() << std::endl;


    } catch (const avro::Exception& e) {
        std::cerr << "Avro exception: " << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Standard exception: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknown exception occurred" << std::endl;
    }
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

void NwSlaClientDeserializeFromAvro(const std::vector<uint8_t>& serialized_data, const std::string& structSchema) {
    try {
        // Compile the Avro schema
        std::istringstream schema_stream(structSchema);
        avro::ValidSchema schema;
        avro::compileJsonSchema(schema_stream, schema);

        // Initialize input stream and decoder
        std::unique_ptr<avro::InputStream> input_stream = avro::memoryInputStream(serialized_data.data(), serialized_data.size());
        avro::DecoderPtr decoder = avro::binaryDecoder();
        decoder->init(*input_stream);

        // Decode Avro datum
        avro::GenericDatum datum(schema);
        avro::decode(*decoder, datum);

        // Access GenericRecord fields
        avro::GenericRecord& record = datum.value<avro::GenericRecord>();
        
        std::cout << "Client Parameters:\n";
        // Print or process fields
        std::cout << "agent_ip: " << record.fieldAt(0).value<std::string>() << std::endl;
        std::cout << "dest_ip: " << record.fieldAt(1).value<std::string>() << std::endl;

        // Handle ipType as an enum
        avro::GenericEnum ipTypeEnum = record.fieldAt(2).value<avro::GenericEnum>();
        int ipTypeIndex = ipTypeEnum.value(); // Get the enum index
        IpType ipTypeValue = static_cast<IpType>(ipTypeIndex);
        std::cout << "ipType: " << to_string(ipTypeValue) << std::endl;

        // Handle protType as an enum
        avro::GenericEnum protTypeEnum = record.fieldAt(3).value<avro::GenericEnum>();
        int protTypeIndex = protTypeEnum.value(); // Get the enum index
        ProtType protTypeValue = static_cast<ProtType>(protTypeIndex);
        std::cout << "protType: " << to_string(protTypeValue) << std::endl;

        std::cout << "interval: " << record.fieldAt(4).value<int>() << std::endl;
        std::cout << "id: " << record.fieldAt(5).value<int>() << std::endl;

        // Handle state as an enum
        avro::GenericEnum stateEnum = record.fieldAt(6).value<avro::GenericEnum>();
        int stateIndex = stateEnum.value(); // Get the enum index
        SessionState stateValue = static_cast<SessionState>(stateIndex);
        std::cout << "state: " << to_string(stateValue) << std::endl;

        std::cout << "port: " << record.fieldAt(7).value<int>() << std::endl;

    } catch (const avro::Exception& e) {
        std::cerr << "Avro exception: " << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Standard exception: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknown exception occurred" << std::endl;
    }
}

void NwSlaSerializeToAvro(const clientparams& params, const latencyresult& result){
  const std::vector<uint8_t> serialized_client =  NwSlaClientSerializeToAvro(params, schema_json_client);
  NwSlaClientDeserializeFromAvro(serialized_client, schema_json_client);
  std::cout << "\n";
  const std::vector<uint8_t> serialized_result =  NwSlaResultSerializeToAvro(result, schema_json_result);
  NwSlaResultDeserializeFromAvro(serialized_result, schema_json_result);
}





