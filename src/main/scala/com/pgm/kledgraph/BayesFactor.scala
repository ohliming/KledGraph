package com.pgm.kledgraph

import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.{Map, Seq, Set}


/**
  * Created by liming on 17-5-8.
  */

class BayesVar(v:Int) {
  var _v:Int = v
  var _parents:Set[BayesVar] = Set()
  var _childs:Set[BayesVar] = Set()

  def num = _parents.size
  def setVar(value :Int) = _v = value
  def addChild(child:BayesVar) = _childs.add(child)
  def addParent(parent:BayesVar) = _parents.add(parent)
}

class BayesFactor(e:BayesVar){
  val _eliminate = e
  var _variables:Seq[BayesVar] = mutable.Seq()
  var _cpdPositive:Seq[Double] = Seq(0.0) // positive
  var _cpdNegative:Seq[Double] = Seq(0.0) // negative
  var _cpds:Seq[Double] = Seq() // merge table
  var _isUsed = false

  def getVariables = this._variables
  def setUsed = _isUsed = true
  def num = this._variables.size
  def addVariable(v:BayesVar) = this._variables = this._variables :+ v
}

class BayesModel {
  var _factors:Set[BayesFactor] = Set() // factors
  def addFactor(variable:BayesFactor) = { _factors.add(variable) }

  def save(hfile:String, sc:SparkContext) = {
    val rdd = sc.makeRDD(_factors.toSeq)
    rdd.saveAsTextFile(hfile)
  }

  def load(hfile:String, sc:SparkContext) = {
    val rdd = sc.textFile(hfile)
  }
}
