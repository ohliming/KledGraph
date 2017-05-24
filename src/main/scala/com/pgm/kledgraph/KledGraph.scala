package com.pgm.kledgraph

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.mllib.linalg.{Matrices, Matrix}

import scala.collection.mutable.{ListBuffer, Map, Seq, Set}
import scala.util.control.Breaks._
import scala.util.parsing.json._

object KledGraph {
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

        // process mapQuestTopic
        if (mapQuestTopic.contains(questionId)){
          mapQuestTopic(questionId).add(topicId)
        }else{
          mapQuestTopic += ((questionId -> Set(topicId)))
        }
      }
    })

    (mapQuestTopic, mapTopicQuest)
  }

  // cache the exceries records
  def getStudRecords(mapQuestTopic: Map[Int, Set[Int]], mapTopic: Map[Int, String], sqlContext: HiveContext) = {
    var listRecords:List[(Int, Int, Int)] = List() // records object
    val rows = sqlContext.sql("select student_id, question_id, result from entity_student_exercise")
    rows.foreach(x => {
      val studentId = x.get(0).toString.toInt
      val questionId = x.get(1).toString.toInt
      var result = x.get(2).toString

      var bFlag:Boolean = false
      if ( mapQuestTopic.contains(questionId) ){
        val setTopic = mapQuestTopic(questionId)
        setTopic.foreach(x => {
          if( mapTopic.contains(x) ){
            bFlag = true
            break
          }
        })
      }

      val regex="""^\d+$""".r  //process effective records
      var res = -1  // init
      if ( bFlag ) {
        if(regex.findFirstMatchIn(result) != None){
          res = result.toInt
        }else {
          val optJson = JSON.parseFull(result)
          optJson match {
            case Some(x) => {
              val mapJson = x.asInstanceOf[scala.collection.immutable.Map[String,Double]]
              res = if(mapJson("result") == 1) 1 else 2
            }
            case None => { res = -1 }
          }
        }

        listRecords = listRecords. +: (studentId, questionId, res) // add list record
      }
    })

    listRecords
  }

  def staticContionPro(listRecords: List[(Int, Int, Int)], mapQuestTopic: Map[Int,Set[Int]], startTopicSet:Set[Int], endTopic:Int, label:Int) = {
    var setStart:Set[Int] = Set() // start student set
    var setEnd:Set[Int] = Set() // end student set
    var mapStudent: Map[Int, Int] = Map() // student set

    listRecords.foreach(x => {
      val studentId = x._1
      val questionId = x._2
      val result = x._3
      if(result == label && mapQuestTopic.contains(questionId)){
        val setTopic = mapQuestTopic(questionId)
        val intersection = setTopic & startTopicSet
        if(intersection.size > 0){setStart.add(studentId)}
        if(setTopic.contains(endTopic)){setEnd.add(studentId)}

        if( mapStudent.contains(studentId)){
          val count = mapStudent(studentId)
          mapStudent.update(studentId, count + 1)
        }else{
          mapStudent += ((studentId -> 1))
        }
      }
    })

    var p0 = 0.0
    var fenzi = 0
    var fenmu = 0
    setStart.foreach(x => {
      if(mapStudent.contains(x)){
        fenmu += mapStudent(x)
        if(setEnd.contains(x)){
          fenzi += mapStudent(x)
        }
      }
    })

    if(fenmu > 0 && fenzi < fenmu){
      p0 = fenzi / fenmu
    }else{
      p0 = 0.0
    }

    var p1 =0.0
    var rfenzi  = 0
    var rfenmu  = 0
    setEnd.foreach(x =>{
      if(mapStudent.contains(x)){
        rfenmu += mapStudent(x)
        if(setStart.contains(x)){
          rfenzi += mapStudent(x)
        }
      }
    })

    if(rfenmu > 0 && rfenmu > rfenzi){
      p1 = rfenzi / rfenmu
    }else{
      p1 = 0.0
    }

    (p0, p1)
  }

  def isLoopGraph(topic1:Int,topic2:Int,initPair:List[(Int, Int)], mapChild:Map[Int,Set[Int]]):Boolean = {
    var listStack:ListBuffer[Int] = ListBuffer(topic1) // stack
    var setMiss:Set[Int] = Set()
    var setPop:Set[Int] = Set()
    var topic = 0
    while( listStack.size > 0 ){
      if(setMiss.contains(topic2)){
        return true
      }

      topic = listStack.last
      listStack = listStack.init
      setPop.add(topic)
      if(mapChild.contains(topic) && !setPop.contains(topic)){
        val childs = mapChild(topic)
        childs.foreach(x => {
          listStack +=  topic
        })
        setMiss.add(topic)
      }
    }

    false
  }

  def structGrahpList(listRecords:List[(Int,Int,Int)], mapTopic:Map[Int, String], mapQuestTopic:Map[Int,Set[Int]],
                      mapTopicQuest:Map[Int,Set[Int]],throld: Int = 30, inDreege:Int = 3, outDreege:Int = 4) = {
    var initPair:List[(Int,Int)]= List()
    var listPair:List[((Int,Int),Int)] = List()
    var mapTemp:Map[Int,Set[Int]] = Map()

    mapTopic.foreach(topic1 => {
      mapTopic.foreach(topic2 => {
        val setCom = if(mapTemp.contains(topic2._1)) mapTemp(topic2._1) else Set(0)
        if(topic1._1 != topic2._1 && setCom.contains(topic1._1)){
          val setQuest1 = mapTopicQuest(topic1._1)
          val setQuest2 = mapTopicQuest(topic2._1)
          val lem = (setQuest1 & setQuest2).size
          if( lem > throld ){
            listPair = listPair. +: ((topic1._1, topic2._1), lem)
            if( mapTemp.contains(topic2._1) ){
              mapTemp(topic2._1).add(topic1._1)
            }else{
              mapTemp += ((topic2._1 -> Set(topic1._1)))
            }
          }
        }
      })
    })

    // add edge for bn
    val listSort = listPair.sortWith(_._2 > _._2)
    var mapChild:Map[Int,Set[Int]] = Map() // cache child
    listSort.foreach(x => {
      var (topic1, topic2) = x._1
      val (p0,p1) = staticContionPro(listRecords,mapQuestTopic,Set(topic1), topic2, 1)
      if(p1 > p0){
        val temp = topic1
        topic1 = topic2
        topic2 = temp
      }

      val bFlag = isLoopGraph(topic1, topic2, initPair, mapChild)
      if(bFlag){
        initPair = initPair. +: (topic1, topic2)
        if(mapChild.contains(topic2)){
          mapChild(topic2).add(topic1) // add topic1
        }else{
          mapChild += ((topic2 -> Set(topic1)))
        }
      }
    })

    initPair
  }

  def makeTopicMatrix(listRecords: List[(Int, Int, Int)], mapQuestTopic:Map[Int,Set[Int]], mapIndex: Map[Int,Int]) = {
    var columns:Seq[Int] = Seq(); var rows:Seq[Int] = Seq() // row and column
    var values:Seq[Double] = Seq()
    var rowCount = 0
    listRecords.foreach(row => {
      val questionId = row._2
      val label = if(row._3==1) 1 else 0
      columns = columns :+ 0
      rows = rows :+ rowCount
      values = values :+ label.toDouble

      if(mapQuestTopic.contains(questionId)){
        val setTopic = mapQuestTopic(questionId)
        setTopic.foreach(topic => {
          val colIndex = mapIndex(topic)
          columns = columns :+ colIndex
          rows = rows :+ rowCount
          values = values :+ 1.0
        })
        rowCount += 1
      }
    })
    Matrices.sparse(listRecords.length, mapIndex.size, columns.toArray, rows.toArray, values.toArray)
  }

  def add(indSeq:Seq[Int]) = {
    indSeq.foreach(x => {
      if(x == 0){
        indSeq.update(indSeq.indexOf(x), 1)
        break
      }else{
        indSeq.update(indSeq.indexOf(x), 0)
      }
    })
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

  def preConditionPro(matrixTopic:Matrix, start:Int, label:Int, variables:Seq[BayesVar], indSeq:Seq[Int], mapIndex:Map[Int,Int]):Double = {
    var fenzi = 0; var fenmu = 0
    var index = 0; val rowsNum = matrixTopic.numRows
    while(index < rowsNum){
      var isFlag:Boolean = true
      for(i <- 0 until indSeq.size){
        val v = matrixTopic.apply(index, mapIndex(variables(i)._v))
        if(v != indSeq(i)){
          isFlag = false
          break
        }
      }

      if( isFlag ){
        val v = matrixTopic.apply(index, start)
        val lex = matrixTopic.apply(index, 0)
        fenmu += 1
        if( v== 1.0 && lex == label ){fenzi += 1}
      }
      index += 1
    }
    val p = if(fenmu == 0 || fenzi > fenmu) 0.0 else fenzi / fenmu
    p
  }

  def staticTopicCPD(mapFactor:Map[Int, BayesFactor], matrixTopic:Matrix, mapIndex:Map[Int,Int]) = {
    mapFactor.foreach(x => { // cal CPD
      val factor = x._2
      val bayes = factor._eliminate
      bayes._parents.foreach(parent => { x._2.addVariable(parent) })
      val variables = x._2.getVariables
      var indSeq:Seq[Int] = Seq(); variables.foreach(x=>{ indSeq = indSeq :+ 0 })
      var index = 0; val border = math.pow(2.0, variables.size)
      while(index < border){
        val p1 = preConditionPro(matrixTopic, x._2._eliminate._v, 1, variables, indSeq, mapIndex)
        val p0 = preConditionPro(matrixTopic, x._2._eliminate._v, 0, variables, indSeq, mapIndex)
        x._2._cpdPositive = x._2._cpdPositive :+ p1
        x._2._cpdNegative = x._2._cpdNegative :+ p0
        index += 1
        add(indSeq)
      }
    })
  }

  def makeMapFactor(mapFactor:Map[Int, BayesFactor],initPair:List[(Int,Int)],setVal:Set[BayesVar]):Unit = {
    initPair.foreach(x => {
      val start = new BayesVar(x._1)
      val end = new BayesVar((x._2))
      if(!mapFactor.contains(x._1)) {
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
      setVal.add(start);setVal.add(end)
    })
  }

  def mapTopic2Index(mapTopic :Map[Int, String]):Map[Int, Int] = {
    var mapIndex:Map[Int, Int] = Map()
    var index = 0
    mapTopic.foreach(topic => {
      mapIndex += ((topic._1->index))
      index += 1
    })
    mapIndex
  }

  def getSequence(setFactor:Set[BayesFactor]) = {
    var variable:Seq[BayesFactor] = Seq()
    setFactor.foreach(factor => {
      variable = variable :+ factor
    })
    variable.sortWith(_._eliminate.num < _._eliminate.num)
  }

  def pos2Seq(x:Int, len:Int) = {
    var indSeq:Seq[Int] = Seq()
    var posLen = len; var position = x
    while(posLen > 0){
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

  def sumPositionsPro(cpds:Seq[Double], posMap:Map[Int,Int], len:Int) = {
    var p = 0.0; var count = 0
    cpds.foreach(pr1=>{
      val index = pos2Seq(count,len)
      var bFlag = true
      posMap.foreach(pair => {
        if(index(pair._1) != pair._2 ){
          bFlag = false
          break
        }
      })

      if(bFlag) p += pr1
      count += 1
    })
    p
  }

  def sumProductEliminateVar(mapFactor:Map[Int,BayesFactor], seqFactor:Seq[BayesFactor], variable: BayesFactor, target: BayesFactor) = {
    val bayes = variable._eliminate
    val setBayesVal = seqFactor.map(x => x._eliminate).toSet
    var factor:BayesFactor = new BayesFactor(bayes)
    var delFactor = mapFactor(bayes._v)

    var parentSet:Set[BayesVar] = Set()
    val parents = bayes._parents;  parents.foreach(x=>{
      if(!setBayesVal.contains(x)){
        factor.addVariable(x)
        parentSet.add(x)
      }
    })

    val childs = bayes._childs; childs.foreach(x=>{if(!setBayesVal.contains(x)){factor.addVariable(x)}})
    val items = factor.getVariables
    var indexSeq:Seq[Int] = Seq();items.foreach(x=>{ indexSeq = indexSeq :+ 0})
    var p:Double  = 1.0
    var index = 0; val border = math.pow(2.0, items.size)
    val eliVariables = delFactor.getVariables
    while( index < border ){
      // parent variable
      var mapIndex:Map[BayesVar,Int] = Map()
      for(pos <- 0 until indexSeq.size){
        mapIndex += ((items(pos), indexSeq(pos)))
      }

      var posMap:Map[Int,Int] = Map()
      for(i <- 0 until  eliVariables.size){
        if(parentSet.contains(eliVariables(i))){
          posMap += ((i -> mapIndex(eliVariables(i))))
        }
      }
      val p1 = sumPositionsPro(delFactor._cpdPositive, posMap, eliVariables.size)
      val p0 = sumPositionsPro(delFactor._cpdNegative, posMap, eliVariables.size)
      p = p0 + p1

      childs.foreach(x=>{ // childs variables
        if(mapIndex.contains(x)){
          val childFactor = mapFactor(x._v)
          var cp1 = 0.0
          val pos = childFactor._variables.indexOf(x)
          if(mapIndex(x) == 1){
            cp1 = sumPositionsPro(childFactor._cpdPositive, Map((pos-> 1)), childFactor._variables.size)
          }else{
            cp1 = sumPositionsPro(childFactor._cpdNegative, Map((pos-> 0)), childFactor._variables.size)
          }
          if(cp1 > 0.0) {
            p= p*cp1
          }
        }
      })

      val variableSet = items.toSet // factors
      seqFactor.foreach(x=> {
        if(x._isUsed == false){
          val fVariable = x.getVariables.toSet
          val diff = fVariable -- variableSet
          if( diff.size == 0){
            var tmpSeq:Seq[Int] = Seq()
            x._variables.foreach(v =>{
              tmpSeq = tmpSeq :+ 1
            })
            val ps = getCPDPosition(tmpSeq)
            p = p * x._cpds(ps)
            x.setUsed
          }
        }
      })

      factor._cpds = factor._cpds :+ p
      index += 1
      add(indexSeq)
    }

    factor
  }

  // conditional probability
  def condSumProductVE(mapFactor:Map[Int,BayesFactor], seqVariable:Seq[BayesFactor], target: BayesFactor,
                       tag:Int /*0~1~-1*/, mapEvidences:Map[BayesVar, Int]) = {
    var seqFactor:Seq[BayesFactor] = Seq()
    var pos = 0; val evidSet = mapEvidences.map(x=>{ mapFactor(x._1._v)}).toSet
    seqVariable.foreach(x=>{  //cut evidences
      if(evidSet.contains(x)){
        seqVariable.drop(pos)
      }
      pos += 1
    })

    seqVariable.foreach(variable => { //loop the variables
      val factor = sumProductEliminateVar(mapFactor, seqFactor, variable, target)
      seqFactor = seqFactor :+ factor
    })

    val targetFactor = seqFactor.last
    var seqIndex:Seq[Int] = Seq()
    targetFactor._variables.foreach(x=>{
      seqIndex = seqIndex :+ mapEvidences(x)
    })

    val targetPos = getCPDPosition(seqIndex)
    val p:Double = if(tag == 1) targetFactor._cpdPositive.apply(targetPos) else targetFactor._cpdNegative.apply(targetPos)

    p
  }

  def makeCliqueTree(mapFactor:Map[Int, BayesFactor]) = {}


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KledGraph") // init the spark
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use neworiental_v3") //use databases

    val mapTopic = getTopic(stageDict("CZ"), subjectDict("cz_chemical"), sqlContext)
    val pair = getQuestionTopic(mapTopic, sqlContext)
    val mapQuestTopic: Map[Int,Set[Int]] = pair._1
    println("the question len is:" + mapQuestTopic.size)
    val mapTopicQuest: Map[Int,Set[Int]] = pair._2
    println("the topic len is:" + mapTopicQuest.size)

    val listRecords = getStudRecords(mapQuestTopic, mapTopic, sqlContext)
    println("the record len is:" + listRecords.length)
    val initPair = structGrahpList(listRecords, mapTopic, mapQuestTopic, mapTopicQuest)
    println("the pair len is:" + initPair.length)

    var setVal:Set[BayesVar] = Set()
    var mapFactor:Map[Int, BayesFactor] =  Map(); makeMapFactor(mapFactor,initPair,setVal)
    var mapIndex:Map[Int, Int] = mapTopic2Index(mapTopic)
    val matrixTopic = makeTopicMatrix(listRecords, mapQuestTopic, mapIndex) // spare matrix
    staticTopicCPD(mapFactor, matrixTopic, mapIndex)

    val model = new BayesModel; mapFactor.foreach(x=>{model.addFactor(x._2)})
    var setFactor:Set[BayesFactor] = Set() // factors set
    mapFactor.foreach(x=>{ setFactor.add(x._2) })

    val sequence = getSequence(setFactor)
    val pos = 1
    val target = sequence(pos); sequence.drop(pos)

    val mapEvidences:Map[BayesVar,Int] = Map() // conditional factors
    val p = condSumProductVE(mapFactor,sequence, target, 1, mapEvidences)

    sc.stop
  }
}