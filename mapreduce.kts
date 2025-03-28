fun mapper(rowKey: String, values: Map<String, String>): List<Pair<String, Any?>> { 
  return values[""]?.split(" ")?.map { it to 1 }?.toList() ?: emptyList()
}

fun reducer(reduceKey: String, values: List<Any>): Any {
  return 1	
}
