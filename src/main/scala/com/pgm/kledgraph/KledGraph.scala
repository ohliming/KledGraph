package com.pgm.kledgraph

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map, Seq, Set}
import scala.util.control._
import scala.util.Random
import org.json4s._
import org.json4s.native.JsonMethods._


object KledGraph {
  val _junctionTree = new JunctionTree // clique tree structure
  val stageDict: Map[String, Int] = Map(
    "CZ"->1,
    "GZ"->2
  )

  val subjectDict: Map[String, Int] = Map(
    "cz_chinese"->1,
    "gz_chinese"->2,
    "cz_math"->3,
    "gz_math"->4,
    "cz_english"->5,
    "gz_english"->6,
    "cz_physical"->7,
    "gz_physical"->8,
    "cz_chemical"->9,
    "gz_chemical"->10,
    "cz_biological"->11,
    "gz_biological"->12,
    "cz_history"->13,
    "gz_history"->14,
    "cz_geographic"->15,
    "gz_geographic"->16,
    "cz_political"->17,
    "gz_political"->18
  )

  val loop = new Breaks
  def getTopic(stage: Int, subject: Int, sqlContext : HiveContext): Map[Int, String] = {
    var mapTopic :Map[Int, String] = Map()
    var sql = "select id, name from entity_topic where subject_id= "+ subject +" and stage_id = " + stage
    val dataRows = sqlContext.sql(sql).collect()
    dataRows.foreach(x => {
      val topicId = x(0).toString.toInt
      val name = x(1).toString
      mapTopic += ((topicId -> name))
    })
    mapTopic
  }

  def getStudents(sqlContext : HiveContext) = {
    val stuSql = "select distinct f.system_id from ( "+
                 "select distinct a.system_id " +
                 "from ( select distinct system_id,org_id from entity_user where type=2 and org_type=2 ) a "+
                 "join ( select org_id from entity_school where enable=1 and private=0 ) b "+
                 "on a.org_id=b.org_id "+
                 "UNION ALL " +
                 "select distinct a.system_id from ( "+
                 "select distinct system_id,org_id from entity_user where type=2 and org_type=4 ) a " +
                 "join ( select org_id from entity_school where enable=1 and private=1 ) b " +
                 "on a.org_id=b.org_id ) f"

    var studSet:Set[Long] = Set()
    val rows = sqlContext.sql(stuSql).collect()
    rows.foreach(x => {
      studSet.add(x.get(0).toString.toLong)
    })

    studSet
  }

  def getQuestionTopic(mapTopic :Map[Int, String],sqlContext: HiveContext):(Map[Int, Set[Int]], Map[Int,Set[Int]]) = {
    var mapQuestTopic: Map[Int, Set[Int]] = Map()
    var mapTopicQuest: Map[Int, Set[Int]] = Map()
    val rows = sqlContext.sql("select topic_id, question_id from link_question_topic").collect()

    rows.foreach(x => {
      val topicId = x(0).toString.toInt
      val questionId = x(1).toString.toInt
      if(mapTopic.contains(topicId)){
        if (mapTopicQuest.contains(topicId)){
          mapTopicQuest(topicId).add(questionId)
        }else{
          mapTopicQuest += ((topicId -> Set(questionId)))
        }

        if (mapQuestTopic.contains(questionId)){ // process mapQuestTopic
          mapQuestTopic(questionId).add(topicId)
        }else{
          mapQuestTopic += ((questionId -> Set(topicId)))
        }
      }
    })

    (mapQuestTopic, mapTopicQuest)
  }

  def getStudRecords(mapQuestTopic: Map[Int, Set[Int]], mapTopic: Map[Int, String], studSet:Set[Long], sqlContext: HiveContext,subjectId:Int,stageId:Int) = {
    var listRecords:List[(Long, Int, Int)] = List() // records object
    var sql = "select a.student_id,a.question_id,a.result from entity_student_exercise as a join link_question_topic as b on " +
      "(b.question_id=a.question_id) join entity_topic as c on (c.id = b.topic_id) where c.subject_id="+subjectId+" and c.stage_id ="+stageId + " and ret_num > 0"
    val rows = sqlContext.sql(sql).collect()
    val setKeyTopic = mapTopic.map(x=>x._1).toSet
    val regex="""^\d+$""".r  //process effective records
    rows.foreach(x => {
      val studentId = x.get(0).toString.toLong
      val questionId = x.get(1).toString.toInt
      var result = x.get(2).toString
      var res = -1  // init

      if ( mapQuestTopic.contains(questionId) && studSet.contains(studentId) ) {
        val setTopic = mapQuestTopic(questionId)
        val setMerge = setKeyTopic & setTopic
        if (setMerge.size > 0) {
          if (regex.findFirstMatchIn(result) != None) {
            res = result.toInt
          } else if (!result.equals("NULL") && !result.equals("") && !result.eq(None)) {
            val t = parse(result.replace("]","").replace("[",""))
            val mapJson = t.values.asInstanceOf[scala.collection.immutable.Map[String,_]]
            if(mapJson.contains("result")){
              if(mapJson("result") != null){
                res =  if(mapJson("result").equals("1")) 1 else 2
              }
            }
          }

          if(res > 0) { //　normal record add the list
            listRecords = listRecords.+:(studentId, questionId, res)
          }
        }
      }
    })

    listRecords
  }

  def staticConditionPro(listRecords: List[(Long, Int, Int)], mapQuestTopic: Map[Int,Set[Int]], startTopicSet:Set[Int], endTopic:Int, label:Int) = {
    var fenmu:Double = 0.0
    var setFenmu:Set[Long] = Set()
    var seqFenzi:Seq[Long] = Seq()

    listRecords.foreach( x => {
      val studentId = x._1
      val questionId = x._2
      val result = x._3
      if(mapQuestTopic.contains(questionId)){
        if(result == label){
          val setTopic = mapQuestTopic(questionId)
          val intersection = setTopic & startTopicSet

          if( intersection.size > 0 ){
            fenmu += 1
            setFenmu.add(studentId)
          }

          if(setTopic.contains(endTopic)){
            seqFenzi = seqFenzi :+ studentId
          }
        }
      }
    })

    var fenzi:Double = 0.0
    seqFenzi.foreach(x=>{
      if(setFenmu.contains(x)){
        fenzi += 1
      }
    })

    var p = 0.0
    if(fenmu > 0){
      p = if(fenzi < fenmu) fenzi / fenmu else 1.0
    }
    p
  }

  def isLoopGraph(topic1:Int, topic2:Int, mapParents:Map[Int,Set[Int]]):Boolean = {
    if( mapParents.contains(topic2) ){ // repetition
      if(mapParents(topic2).contains(topic1)){
        return  true
      }
    }

    var listStack:ListBuffer[Int] = ListBuffer(topic1) // stack
    var setMiss:Set[Int] = Set()
    var setPop:Set[Int] = Set()
    var topic = 0
    while( listStack.size > 0 ){
      topic = listStack.last
      setMiss.add(topic)
      listStack = listStack.init
      if( mapParents.contains(topic) && !setPop.contains(topic) ){
          mapParents(topic).foreach(x => { listStack +=  x })
      }

      setPop.add(topic)
    }

    val isLoop = if(setMiss.contains(topic2)) true else false
    isLoop
  }

  def structGrahpList(listRecords:List[(Long,Int,Int)], mapTopic:Map[Int, String], mapQuestTopic:Map[Int,Set[Int]],
                      mapTopicQuest:Map[Int,Set[Int]],throld: Int = 30, inDreege:Int = 3, outDreege:Int = 4) = {
    var listPair:List[((Int,Int),Int)] = List()
    mapTopic.foreach(topic1 => {
      mapTopic.foreach(topic2 => {
        val flag:Boolean = mapTopicQuest.contains(topic1._1) && mapTopicQuest.contains(topic2._1)
        if(topic1._1 != topic2._1 && flag){
          val setQuest1 = mapTopicQuest(topic1._1)
          val setQuest2 = mapTopicQuest(topic2._1)
          val lem = (setQuest1 & setQuest2).size
          if( lem > throld ){ listPair = listPair. +: ((topic1._1, topic2._1), lem)}
        }
      })
    })

    val listSort = listPair.sortWith(_._2 > _._2)
    var mapParents:Map[Int,Set[Int]] = Map() // cache child
    var mapChilds:Map[Int,Set[Int]] = Map()
    var initPair:List[(Int,Int)]= List()
    listSort.foreach(x => {
      var (topic1, topic2) = x._1
      val p0 = staticConditionPro(listRecords,mapQuestTopic,Set(topic1), topic2, 1)
      val p1 = staticConditionPro(listRecords,mapQuestTopic,Set(topic2), topic1, 1)
      if(p1 > p0){
        val temp = topic1
        topic1 = topic2
        topic2 = temp
      }

      val bFlag = isLoopGraph(topic1, topic2, mapParents)
      val inCnt = if(mapParents.contains(topic2)) mapParents(topic2).size else 0
      val outCnt = if(mapChilds.contains(topic1)) mapChilds(topic1).size else 0

      if(!bFlag && inCnt < inDreege && outCnt < outDreege ){
        initPair = initPair. +: (topic1, topic2)
        if(mapParents.contains(topic2)){
          mapParents(topic2).add(topic1)
        }else{
          mapParents += ((topic2 -> Set(topic1)))
        }

        if(mapChilds.contains(topic1)){
          mapChilds(topic1).add(topic2)
        }else{
          mapChilds += ((topic1 -> Set(topic2)))
        }
      }
    })

    initPair
  }

  def makeTopicMatrix(listRecords: List[(Long, Int, Int)], mapQuestTopic:Map[Int,Set[Int]], mapIndex: Map[Int,Int], mapTopic:Map[Int,String]) = {
    var resVectors:Seq[Vector ] = Seq()
    var mapRowStudent:Map[Int, Long] = Map()
    var index = 0
    listRecords.foreach(record =>{
      val questionId = record._2
      val studentId  = record._1
      val label =  if(record._3 ==1) 1.0 else 0.0
      if(mapQuestTopic.contains(questionId)){
        var posArr:ListBuffer[Int] = new ListBuffer()
        var valArr:ListBuffer[Double] = new ListBuffer()

        posArr += 0
        valArr += label
        val topics = mapQuestTopic(questionId)
        if(topics.size > 0){
          topics.foreach(topic => {
            if(mapIndex.contains(topic)){
              posArr += mapIndex(topic)
              valArr += 1.0
            }
          })

          mapRowStudent += ((index -> studentId ))
          index += 1
          resVectors = resVectors :+ Vectors.sparse(mapIndex.size+1, posArr.sortWith(_<_).toArray, valArr.toArray)
        }
      }
    })

    (resVectors, mapRowStudent)
  }

  def addSeq(indSeq:Seq[Int]) = {
    loop.breakable {
      for(pos <- 0 until indSeq.size){
        if(indSeq(pos) == 0){
          indSeq.update(pos, 1)
          loop.break
        }else{
          indSeq.update(pos, 0)
        }
      }
    }
  }

  def getCPDPosition(indSeq:Seq[Int]):Int = {
    var pos:Int = 0
    var count = 0
    indSeq.foreach(x=>{
      if(x == 1){
        pos += math.pow(2.0,count).toInt
      }
      count += 1
    })

    pos
  }

  def randomSet(n:Int, maxIndex:Int)={
    var resultSet:Set[Int]= Set()
    while(resultSet.size < n){
      val randomNum=(new Random).nextInt(maxIndex:Int)
      if(!resultSet.contains(randomNum)){
        resultSet.add(randomNum)
      }
    }
    resultSet
  }

  def preConditionPro(vecRecords:Seq[Vector], mapRowStudent:Map[Int, Long], topic:Int, position:Int, label:Int, variables:Seq[BayesVar],
                      indSeq:Seq[Int], mapIndex:Map[Int,Int], threshold:Double = 0.2):Double = {
    var index = 0
    var setFenmu:Set[Long] = Set()
    var seqFenzi:Seq[Long] = Seq()

    var fenmu:Double = 0
    var fenzi:Double = 0
    vecRecords.foreach(record => {
      if(mapRowStudent.contains(index)){
        var isFenmu = true
        val studentId = mapRowStudent(index)
        loop.breakable {
          for(i<- 0 until variables.size){
            val v = record.apply(mapIndex(variables(i)._v))
            if( v != indSeq(i) ){
              isFenmu = false
              loop.break
            }
          }
        }

        if(isFenmu){
          fenmu += 1
          setFenmu.add(studentId)
        }

        val value  = record.apply(position)
        val target = record.apply(0)
        if(value == 1.0 && target == label){
          if(isFenmu){
            fenzi += 1
          }else{
            seqFenzi = seqFenzi :+ studentId
          }
        }
      }

      index += 1
    })

    var p:Double = 0.0
    if( fenmu > 0 ){
      p = if(fenzi < fenmu) fenzi/fenmu else 1.0
    }

    if( p < threshold && p > 0 && seqFenzi.size > 0 ){
      var cnt = 0
      seqFenzi.foreach(studId => {
        if(setFenmu.contains(studId)){
          cnt += 1
        }
      })

      val pro = (cnt / seqFenzi.size) * (1-threshold)
      p = p + pro
    }

    p
  }

  def staticTopicCPD(mapFactor:Map[Int, BayesFactor], vecRecords:Seq[Vector],mapRowStudent:Map[Int, Long], mapIndex:Map[Int,Int], mapTopic:Map[Int,String]) = {
    mapFactor.foreach(x => { // cal cpd
      val bayes = x._2._eliminate
      bayes._parents.foreach(parent => { x._2.addVariable(parent) })
      val variables = x._2.getVariables
      var indSeq:Seq[Int] = Seq(); variables.foreach(x=>{ indSeq = indSeq :+ 0})
      if(variables.size > 0) {
        var index = 0
        val border = math.pow(2.0, variables.size)
        if(mapIndex.contains(x._2._eliminate._v)){
          val topicIndex = mapIndex(x._1)
          while( index < border ){
            val p1 = preConditionPro(vecRecords, mapRowStudent, x._1, topicIndex, 1, variables, indSeq, mapIndex)
            val p0 = 1-p1
            x._2._cpdPositive = x._2._cpdPositive :+ p1
            x._2._cpdNegative = x._2._cpdNegative :+ p0
            index += 1
            addSeq(indSeq)
          }
        }
      }
    })
  }

  def makeMapFactor(mapFactor:Map[Int, BayesFactor], initPair:List[(Int,Int)]):Unit = {
    var mapVal:Map[Int, BayesVar] = Map()
    initPair.foreach(x => {
      val start =  if(mapVal.contains(x._1)) mapVal(x._1) else new BayesVar(x._1)
      val end = if(mapVal.contains(x._2)) mapVal(x._2) else new BayesVar((x._2))

      if( !mapFactor.contains(x._1) ) {
        start.addChild(end)
        mapFactor += ((x._1-> new BayesFactor(start)))
      }else{
        start.addChild(end)
      }

      if(!mapFactor.contains(x._2)){
        end.addParent(start)
        mapFactor += ((x._2 -> new BayesFactor(end)))
      }else{
        end.addParent(start)
      }
      mapVal.update(x._1, start)
      mapVal.update(x._2, end)
    })
  }

  def mapTopic2Index(mapTopic :Map[Int, String]):Map[Int, Int] = {
    var mapIndex:Map[Int, Int] = Map()
    var index = 1
    mapTopic.foreach(topic => {
      mapIndex += (( topic._1 -> index ))
      index += 1
    })
    mapIndex
  }

  def gradationSort(vSort:ArrayBuffer[BayesFactor]) = {
    var res:Seq[BayesFactor] = Seq()
    var tBuf:ArrayBuffer[BayesFactor] = ArrayBuffer()
    var preV = 0
    var nowV = 0
    vSort.foreach(x=> {
      nowV = x._eliminate.num
      if(preV != nowV && tBuf.size > 0){
        res ++= tBuf.sortWith(_._eliminate._v < _._eliminate._v)
        tBuf.clear()
      }

      tBuf += x
      preV = nowV
    })

    res ++= tBuf.sortWith(_._eliminate._v < _._eliminate._v)
    res
  }

  def getSequence(setFactor:Set[BayesFactor], mapEvidences:Map[BayesVar,Int], v:Int) = {
    var vGeneral:ArrayBuffer[BayesFactor] = ArrayBuffer()
    var vVariable:ArrayBuffer[BayesFactor] = ArrayBuffer()
    val setEvid = mapEvidences.map(x=> x._1).toSet
    setFactor.foreach(factor => {
      if(!setEvid.contains(factor._eliminate) && factor._eliminate._v != v){
        val fSet = factor.getVariables.map(x => x._v).toSet
        if(fSet.contains(v)){
          vVariable += factor
        }else{
          vGeneral += factor
        }
      }
    })

    var vSort2 = vVariable.sortWith(_._eliminate.num < _._eliminate.num)
    var vSort1 = vGeneral.sortWith(_._eliminate.num < _._eliminate.num)
    var res1 = gradationSort(vSort1)
    var res2 = gradationSort(vSort2)
    res1 ++ res2
  }

  def pos2Seq(x:Int, len:Int) = {
    var indSeq:Seq[Int] = Seq()
    var posLen = len-1
    var position = x
    while(posLen >= 0){
      val v = math.pow(2.0, posLen).toInt
      if(position >= v) {
        position = position - v
        indSeq = indSeq :+ 1
      }else{
        indSeq = indSeq :+ 0
      }
      posLen += -1
    }
    indSeq.reverse
  }

  def sumPositionsPro(cpds:Seq[Double], pMap:Map[Int,Int], len:Int) = {
    var p = 1.0
    var cnt = 0
    cpds.foreach(pi=>{
      val index = pos2Seq(cnt, len)
      var bFlag = true
      loop.breakable {
        pMap.foreach(pair => {
          if(index(pair._1) != pair._2 ) {
            bFlag = false
            loop.break
          }
        })
      }

      if( bFlag ){
        p = p * pi
      }
      cnt += 1
    })
    p
  }

  def getTopChilds(childs:Set[BayesVar], mapFactor:Map[Int,BayesFactor], setBayes:scala.collection.immutable.Set[BayesVar], vSet:Set[BayesVar],
                   itemsVSet:Set[Int], bayes: BayesVar, n:Int) = { // process childs
    var childMap:Map[BayesVar,Double] = Map()
    childs.foreach( x=> {
      if(!setBayes.contains(x)){
        var indexSeq:Seq[Int] = Seq()
        val f = mapFactor(x._v)
        val xVariable = f.getVariables
        xVariable.foreach(v => {
          itemsVSet.add(v._v)
          if(v.eq(bayes)){
            indexSeq = indexSeq :+ 1
          }else{
            indexSeq = indexSeq :+ 0
          }
        })

        val pos = getCPDPosition(indexSeq)
        val p1 = f._cpdPositive(pos)
        childMap += ((x -> p1))
      }
    })

    val sortMap = childMap.toSeq.sortWith(_._2>_._2)
    val range = if(sortMap.size > n) n else sortMap.size
    for(i <- 0 until range){
      val child = sortMap(i)._1
      val xparent = child._parents
      xparent.foreach(x=> {
        if(!setBayes.contains(x)){
          vSet.add(x)
        }
      })
    }
  }

  def sumProductEliminateVar(mapFactor:Map[Int,BayesFactor], sFactor:Seq[BayesFactor], setBayes:Seq[BayesVar], variable: BayesFactor) = {
    val bayes = variable._eliminate
    val setBayesVal = setBayes.toSet
    var factor: BayesFactor = new BayesFactor(bayes)
    var delFactor = mapFactor(bayes._v)

    var parentSet:Set[BayesVar] = Set()
    val parents = bayes._parents
    var varSet:Set[BayesVar] = Set()
    parents.foreach(x=>{
      if(!setBayesVal.contains(x)){
        parentSet.add(x)
        varSet.add(x)
      }
    })

    val childs = bayes._childs
    var itemsVSet:Set[Int] = Set(); parents.foreach(x=> itemsVSet.add(x._v))
    getTopChilds(childs, mapFactor, setBayesVal, varSet, itemsVSet, bayes, 2)
    val varSort = varSet.toSeq.sortWith(_._v < _._v)
    varSort.foreach(v => factor.addVariable(v)) // add variables
    var items = factor.getVariables
    var p:Double  = 0.0 // result
    var index = 0; val border = math.pow(2.0, items.size)
    val eliVariables = delFactor.getVariables
    var indexSeq:Seq[Int] = Seq(); items.foreach( x =>{ indexSeq = indexSeq :+ 0 })
    if( items.size > 0 ){
      while( index < border ) {
        var map2Index:Map[BayesVar,Int] = Map()
        for(pos <- 0 until items.size){ map2Index += ((items(pos) -> indexSeq(pos))) }
        if( eliVariables.size > 0 ){
          var posMap:Map[Int,Int] = Map()
          for( i <- 0 until eliVariables.size ){
            if(parentSet.contains(eliVariables(i))){
              posMap += (( i -> map2Index(eliVariables(i)) ))
            }
          }

          if( posMap.size > 0 ){
            val p1 = sumPositionsPro(delFactor._cpdPositive, posMap, eliVariables.size)
            val p0 = sumPositionsPro(delFactor._cpdNegative, posMap, eliVariables.size)
            p = p0 + p1
          }
        }

        childs.foreach(c=>{ // childs variables
          if( map2Index.contains(c) ){
            val childFactor = mapFactor(c._v)
            var iSeq:Seq[Int] = Seq()
            var tpos = 0
            for(i <- 0 until childFactor._variables.size){
              val b = childFactor._variables(i)
              if(b.eq(bayes)){
                tpos = i
                iSeq = iSeq :+ 0
              }else{
                if( map2Index.contains(b) ) {
                  iSeq = iSeq :+ map2Index(b)
                }else{
                  iSeq = iSeq :+ 1
                }
              }
            }

            val pos0 = getCPDPosition(iSeq)
            iSeq.update(tpos, 1)
            val pos1 = getCPDPosition(iSeq)
            var cp1 = (childFactor._cpdPositive(pos1) * childFactor._cpdPositive(pos0))
            cp1 += (childFactor._cpdNegative(pos1) * childFactor._cpdNegative(pos0))
            if( cp1 > 0.0 ){
              p = if(p > 0) p * cp1 else cp1
            }
          }
        })

        sFactor.foreach(x=> {
          if( x._isUsed >= 0 ){
            val fVariable = x.getVariables.map(x=>x._v).toSet
            val diff = fVariable -- itemsVSet
            if( diff.size == 0 ){
              var tmpSeq:Seq[Int] = Seq()
              x._variables.foreach(v => {
                if(map2Index.contains(v)){
                  tmpSeq = tmpSeq :+ map2Index(v)
                }else{
                  tmpSeq = tmpSeq :+ 0
                }
              })

              val ps = getCPDPosition(tmpSeq)
              if(ps < x._cpds.size) {
                p =  if(p > 0.0) p * x._cpds(ps) else x._cpds(ps)
              }

              x._isUsed = 1
            }
          }
        })

        factor._cpds = factor._cpds :+ p
        index += 1
        addSeq(indexSeq)
      }
    }

    sFactor.foreach(x=>{ if(x._isUsed  == 1){x.setUsed}})
    factor
  }

  def mapMaxSumProduct( mapFactor:Map[Int,BayesFactor], seqVariable:Seq[BayesFactor] ) = {    // make clique tree
    var nodes:Seq[BayesFactor] = Seq()
    var setBayesVar:Seq[BayesVar] = Seq()
    seqVariable.foreach(x => {
      setBayesVar = setBayesVar :+ x._eliminate
      val factor = sumProductEliminateVar(mapFactor, nodes, setBayesVar, x)
      if( factor._cpds.size >  0 ){
        nodes = nodes :+ factor
      }
    })

    val edge:Map[BayesFactor, BayesFactor] = Map()
    var nodeSet:Set[Int] = Set(); nodes.foreach(x => nodeSet.add(x._eliminate._v))
    var node = mapFactor(nodeSet.last)
    while(nodeSet.size > 0){ // make edge
      val v = node._eliminate._v
      nodeSet.remove(v)
      var vs1 = node.getVariables.map( x => x._v )
      vs1 = vs1 :+ v

      var inCnt = 0
      nodeSet.foreach(target => {
        var vs2 = mapFactor(target).getVariables.map( x => x._v)
        vs2 = vs2 :+ target
        val cnt = (vs1.toSet & vs2.toSet).size
        if( cnt > inCnt ){
          val source = mapFactor(target)
          inCnt = cnt
          if(edge.contains(node)){
            edge.update(node, source)
          }else{
            edge += ((node -> source))
          }
        }
      })

      if( nodeSet.size > 0 ) {
        if(edge.contains(node)){
          node = edge(node)
        }else{
          val source = mapFactor(nodeSet.last)
          edge += ((node -> source))
          node = source
        }
      }
    }

    // construction of a cluster tree
    nodes
  }

  def condSumProductVE(mapFactor:Map[Int,BayesFactor], seqVariable:Seq[BayesFactor], target: BayesFactor,
                       tag:Int /*0~1~-1*/, mapEvidences:Map[BayesVar, Int]) = {  // conditional probability
    var seqFactor:Seq[BayesFactor] = Seq()
    var pos = 0; val evidSet = mapEvidences.map(x=>{ mapFactor(x._1._v)}).toSet
    seqVariable.foreach(x=>{ // cut evidences
      if(evidSet.contains(x)){
        seqVariable.drop(pos)
      }
      pos += 1
    })

    var setBayesVar:Seq[BayesVar] = Seq()
    seqVariable.foreach(variable => { // loop the variables
      setBayesVar = setBayesVar :+ variable._eliminate
      val factor = sumProductEliminateVar(mapFactor, seqFactor, setBayesVar, variable)
      if(factor._cpds.size > 0){
        seqFactor = seqFactor :+ factor
      }
    })

    val targetFactor = seqFactor.last
    var p:Double = targetFactor._cpds.last
    if( mapEvidences.size > 0 ){ // condition
      var pos = 0
      var posMap:Map[Int,Int] = Map()
      targetFactor._variables.foreach(x => {
        if(mapEvidences.contains(x)){
          posMap += ((pos -> mapEvidences(x)))
        }
        pos += 1
      })

      var index = 0
      var sump:Double = 0.0
      targetFactor._cpds.foreach(d => {
        val arr = pos2Seq(index, targetFactor._variables.size)
        var bFlag = true
        loop.breakable {
          posMap.foreach( x =>{
            if( arr(x._1) != x._2 ){
              bFlag = false
              loop.break
            }
          })
        }

        if(bFlag){sump += d}
        index += 1
      })

      if( sump > 0.0 ){
        p = p / sump
      }
    }

    p
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KledGraph") // init the spark
    val sc = new SparkContext(conf)
    println("the applications id is " + sc.applicationId)
    val sqlContext = new HiveContext(sc)
    // sqlContext.sql("use neworiental_v3") // use databases

    val mapTopic = getTopic(stageDict("CZ"), subjectDict("cz_chemical"), sqlContext)
    val pair = getQuestionTopic(mapTopic, sqlContext)
    val mapQuestTopic: Map[Int,Set[Int]] = pair._1
    println("the question len is:" + mapQuestTopic.size)

    val mapTopicQuest: Map[Int,Set[Int]] = pair._2
    println("the topic len is:" + mapTopicQuest.size)

    val studSet = getStudents(sqlContext)
    println("the student count is:"+studSet.size)

    val listRecords = getStudRecords(mapQuestTopic, mapTopic, studSet, sqlContext, subjectDict("cz_chemical"), stageDict("CZ"))
    println("the record len is:" + listRecords.length)

    var mapIndex:Map[Int, Int] = mapTopic2Index(mapTopic)
    println("the map index len is:"+mapIndex.size)

    val initPair = structGrahpList(listRecords, mapTopic, mapQuestTopic, mapTopicQuest)
    println("the pair len is:" + initPair.length)

    val (vecRecords, mapRowStudent) = makeTopicMatrix(listRecords, mapQuestTopic, mapIndex, mapTopic) // spare matrix
    println("the vec size:"+vecRecords.size + " and mapRowstudent len is:" + mapRowStudent.size)

    var mapFactor:Map[Int, BayesFactor] =  Map(); makeMapFactor(mapFactor, initPair)
    println("the init factor len is:"+mapFactor.size)

    staticTopicCPD(mapFactor, vecRecords, mapRowStudent, mapIndex, mapTopic)
    println("the cpd factor len is:"+ mapFactor.size)
    val model = new BayesModel; mapFactor.foreach(x=>{ model.addFactor(x._2) })
    model.save("./liming/BayeModel", sc)

    var setFactor:Set[BayesFactor] = Set()
    mapFactor.foreach( x=> { setFactor.add(x._2) })

    val _v = 15115
    val target = mapFactor(_v) // conditional factors
    val parentTarget =  target._eliminate._parents.map(x=>x._v).toSeq
    println("the parent is:"+ parentTarget)

    val mapEvidences:Map[BayesVar, Int] = Map(mapFactor(parentTarget(0))._eliminate -> 1, mapFactor(parentTarget(1))._eliminate ->1)
    val sequence = getSequence(setFactor, mapEvidences, _v)
    println("the sequence and size is:"+sequence.size)

    val p = condSumProductVE(mapFactor, sequence, target, 1, mapEvidences)
    println("the result p=" + p)

    mapMaxSumProduct(mapFactor, sequence) // map struct for pgm
    sc.stop
  }
}
