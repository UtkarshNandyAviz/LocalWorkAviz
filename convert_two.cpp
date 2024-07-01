 #include "cpx.hh"
 #include "avro/Encoder.hh"
 #include "avro/Decoder.hh"
 

 int main(){
    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);

    clientparams c1;
    c1.agent_ip = "1.10.23.111";
    c1.dest_ip = "1.67.21.345";
    c1.ipType = IpType::IPTYPE_IPV4;
    c1.protType = ProtType::PROTTYPE_ICMP;
    c1.interval = 2;
    c1.id = 1;
    c1.state = SessionState::SESSION_STATE_RUNNING;
    c1.port = 4000;

    avro::encode(*e, c1);

    std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(*out);
    avro::DecoderPtr d = avro::binaryDecoder();
    d->init(*in);

    clientparams c2;
    avro::decode(*d, c2);
    std::cout << '(' << c2.agent_ip << ", " << c2.dest_ip << ", " << c2.id << ')' << std::endl;


    return 0;
 }