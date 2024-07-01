#include "convert.hh"

int main(int argc, char* argv[]){
    const std::vector<uint8_t> serialized_data =  NwSlaClientSerializeToAvro(cp, schema_json_client);
    NwSlaClientDeserializeFromAvro(serialized_data, schema_json_client);
}








