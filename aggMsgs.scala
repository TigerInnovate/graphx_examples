
/**
这段代码有几个疑问需要进一步探索。
1. 为什么对VertexRDD进行mapValues操作后，collect后还是(1, null)的null
2. 对VertexRDD进行map操作，为什么只能用变量x，而不能用元祖(vid, data)
3. aggregateMessage对没有收到消息的vertex进行merge后，为什么Int是0，而其他类型是null
*/
import org.apache.spark.graphx._
val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
 (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))
val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
 Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
 Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
val myGraph = Graph(myVertices, myEdges)

case class Myvd(val cnt: Int=0, val changed: Boolean=false){
    def apply() = new Myvd(0, false)
}


val initialGraph = myGraph.mapVertices((vid,data) => Myvd(0, false))



def sendMsg(ec: EdgeContext[Myvd,String,Myvd]): Unit = {
  ec.sendToDst(Myvd(ec.srcAttr.cnt+1, ec.srcAttr.cnt+1 > ec.dstAttr.cnt))
}

def mergeMsg(a: Myvd, b: Myvd) = {
  if(a.cnt > b.cnt) a else b
}

def propagateEdgeCount(g:Graph[Myvd,String]):Graph[Myvd,String] = {    
  
  val ngv = g.vertices.mapValues{(data:Myvd) => if(data != null) data else Myvd(0, false)}
  val ng = Graph(ngv, g.edges)
  val v2 = ng.aggregateMessages[Myvd](sendMsg, mergeMsg)
  //val v2 = v.map{( vid:VertexId, data:Myvd ) => (vid, Myvd(99, true))} //if(data != null) data else Myvd(0, false)}
  val g2 = Graph(v2, g.edges)
  
  val check = g2.vertices.map[Boolean]{ x =>if(x._2!=null) x._2.changed else false}.reduce( _ || _ ) 
  
  if (check) 
    propagateEdgeCount(g2)
  else
    g
}

initialGraph.vertices.collect
val ng = propagateEdgeCount(initialGraph)
ng.vertices.collect
ng.edges.collect
