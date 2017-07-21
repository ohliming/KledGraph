package com.pgm.kledgraph

import org.apache.spark.SparkContext
import scala.collection.mutable.{Map, Seq, Set}

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL._
/**
  * Created by liming on 17-5-8.
  */

class BayesVar(v:Int) {
  var _v:Int = v
  var _parents:Set[BayesVar] = Set()
  var _childs:Set[BayesVar] = Set()

  def num = _parents.size + _childs.size
  def setVar(value :Int) = _v = value
  def addChild(child:BayesVar) = _childs.add(child)
  def addParent(parent:BayesVar) = _parents.add(parent)
}

class BayesFactor(e:BayesVar){
  var _eliminate = e
  var _variables:Seq[BayesVar] = Seq()
  var _cpdPositive:Seq[Double] = Seq() // positive
  var _cpdNegative:Seq[Double] = Seq() // negative
  var _cpds:Seq[Double] = Seq() // merge table
  var _isUsed = 0

  def getVariables = this._variables
  def setUsed = { _isUsed = -1 }
  def num = this._variables.size
  def addVariable(v:BayesVar) = {this._variables = this._variables :+ v}
}

class BayesModel {
  var _factors:Set[BayesFactor] = Set() // factors
  def addFactor(variable:BayesFactor) = { _factors.add(variable) }

  def save(hFile:String, sc:SparkContext) = {
    var resSeq:Seq[String] = Seq()
    _factors.foreach(factor => {
      var mapRes:Map[String, String] = Map()
      mapRes += (( "cpdNegative" -> factor._cpdNegative.toString ))
      mapRes += (( "cpdPositive" -> factor._cpdPositive.toString ))
      mapRes += (( "cpds" -> factor._cpds.toString ))
      mapRes += (( "variables" -> factor._variables.map(x=>x._v).toString ))
      mapRes += (( "eliParents" -> factor._eliminate._parents.map(x=>x._v).toString ))
      mapRes += (( "eliChilds" -> factor._eliminate._childs.map(x=>x._v).toString ))
      mapRes += (( "eliV" -> factor._eliminate._v.toString ))

      val jsonMap = compact(render(mapRes)).toString
      resSeq = resSeq :+ jsonMap
    })

    val rdd = sc.makeRDD(resSeq)
    rdd.saveAsTextFile(hFile)
  }

  def load(hFile:String, sc:SparkContext) = {
    val rdd = sc.textFile(hFile)
    var mapVal:Map[Int,BayesVar] = Map()
    rdd.foreach(line =>{
      var bayes = new BayesVar(0)
      var factor = new BayesFactor(bayes)

      var t = parse(line)
      for( (k,v) <- t.values.asInstanceOf[Map[String,String]]) {
        if(k.equals("eliV")){
          bayes.setVar(v.toInt)
        }else if(k.equals("eliParents")){
          val parents = v.replace("List(","").replace(")","").split(",").map(x=> x.toInt)
          parents.foreach(x=>{
            val pBayes = if(mapVal.contains(x)) mapVal(x) else new BayesVar(x)
            pBayes.addChild(bayes)
            bayes.addParent(pBayes)
            mapVal.update(x, pBayes)
          })
        }else if(k.equals("variables")) {
          val variables = v.replace("List(","").replace(")","").split(",").map(x=> x.toInt)
          variables.foreach(x => {
            val pBayes = if(mapVal.contains(x)) mapVal(x) else new BayesVar(x)
            factor.addVariable(pBayes)
            mapVal.update(x,pBayes)
          })
        }else if(k.equals("cpds")){
          val cpds = v.replace("List(","").replace(")","").split(",").map(x=> x.toDouble)
          cpds.foreach(x=>{factor._cpds = factor._cpds :+ x})
        }else if(k.equals("cpdPositive")){
          val cpdsPositive = v.replace("List(","").replace(")","").split(",").map(x=> x.toDouble)
          cpdsPositive.foreach(x=>{factor._cpdPositive = factor._cpds :+ x})
        }else if(k.equals("cpdNegative")){
          val cpdNegative = v.replace("List(","").replace(")","").split(",").map(x=> x.toDouble)
          cpdNegative.foreach(x=>{factor._cpdNegative = factor._cpds :+ x})
        }
      }

      this.addFactor(factor)
    })
  }
}
