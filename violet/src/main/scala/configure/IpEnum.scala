package configure

object IpEnum extends Enumeration{

  type IpEnum = Value
  val DevelopEnv = Value("172.")
  val TestEnv = Value("192.")
  val FormalEnv = Value("10.")
}
