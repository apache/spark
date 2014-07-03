/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.ann

import java.awt._
import java.awt.event._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import scala.Array.canBuildFrom

object windowAdapter extends WindowAdapter {
  
  override def windowClosing( e: WindowEvent ) {
    System.exit(0)
  }
  
}

class OutputCanvas2D( wd: Int, ht: Int ) extends Canvas {
  
  var points: Array[Vector] = null
  var approxPoints: Array[Vector] = null
    
  /* input: rdd of (x,y) vectors */
  def setData( rdd: RDD[Vector] ) {
    points = rdd.toArray  
    repaint
  }
    
  def setApproxPoints( rdd: RDD[Vector] ) {      
    approxPoints = rdd.toArray
    repaint
  }
    
  def plotDot( g: Graphics, x: Int, y: Int ) {
    val r = 5  
    val noSamp = 6*r
    var x1 = x
    var y1 = y+r
    for( j<-1 to noSamp ) {
      val x2 = (x.toDouble+math.sin( j.toDouble*2*math.Pi/noSamp )*r+.5).toInt
      val y2 = (y.toDouble+math.cos( j.toDouble*2*math.Pi/noSamp )*r+.5).toInt
      g.drawLine( x1, ht-y1, x2, ht-y2 )
      x1 = x2
      y1 = y2
    }
  }
  
  override def paint( g: Graphics) = {
	  
	  var xmax: Double = 0.0
	  var xmin: Double = 0.0
	  var ymax: Double = 0.0
	  var ymin: Double = 0.0
	  
	  if( points!=null ) {
	  
	    g.setColor( Color.black )
	    val x = points.map( T => (T.toArray)(0) )
	    val y = points.map( T => (T.toArray)(1) )
		  
	    xmax = x.max
	    xmin = x.min
	    ymax = y.max
	    ymin = y.min
		  
	    for( i <- 0 to x.size-1 ) {
	    
	      val xr = (((x(i).toDouble-xmin)/(xmax-xmin))*wd+.5).toInt
	      val yr = (((y(i).toDouble-ymin)/(ymax-ymin))*ht+.5).toInt	    
	      plotDot( g, xr, yr )
	    
	    }
	  
      if( approxPoints != null ) {
        
	      g.setColor( Color.red )
		    val x = approxPoints.map( T => (T.toArray)(0) )
		    val y = approxPoints.map( T => (T.toArray)(1) )
		
		    for( i <- 0 to x.size-1 ) {
		      val xr = (((x(i).toDouble-xmin)/(xmax-xmin))*wd+.5).toInt
		      val yr = (((y(i).toDouble-ymin)/(ymax-ymin))*ht+.5).toInt
		      plotDot( g, xr, yr )
		    }
		    
	    }

	  }
	  
  }
}

class OutputFrame2D( title: String ) extends Frame( title ) {
  
  val wd = 800
  val ht = 600
  
  var outputCanvas = new OutputCanvas2D( wd, ht )
  
  def apply() {
    addWindowListener( windowAdapter )
    setSize( wd, ht )
    add( "Center", outputCanvas )
    show()
  }
  
  def setData( rdd: RDD[Vector] ) {
    outputCanvas.setData( rdd )
  }

  def setApproxPoints( rdd: RDD[Vector] ) {
    outputCanvas.setApproxPoints( rdd )
  }

  
}

object windowAdapter3D extends WindowAdapter {
  
  override def windowClosing( e: WindowEvent ) {
    System.exit(0)
  }
  
}

class OutputCanvas3D( wd: Int, ht: Int, shadowFrac: Double ) extends Canvas {
 
  var angle: Double = 0
  var points: Array[Vector] = null
  var approxPoints: Array[Vector] = null

  /* 3 dimensional (x,y,z) vector */
  def setData( rdd: RDD[Vector] ) {
    points = rdd.toArray  
    repaint
  }
    
  def setApproxPoints( rdd: RDD[Vector] ) {      
    approxPoints = rdd.toArray
    repaint
  }
    
  def plotDot( g: Graphics, x: Int, y: Int ) {
    val r = 5  
    val noSamp = 6*r
    var x1 = x
    var y1 = y+r
    for( j<-1 to noSamp ) {
      val x2 = (x.toDouble+math.sin( j.toDouble*2*math.Pi/noSamp )*r+.5).toInt
      val y2 = (y.toDouble+math.cos( j.toDouble*2*math.Pi/noSamp )*r+.5).toInt
      g.drawLine( x1, ht-y1, x2, ht-y2 )
      x1 = x2
      y1 = y2
    }
  }
    
  def plotLine( g: Graphics, x1: Int, y1: Int, x2: Int, y2: Int ) {
    g.drawLine( x1, ht-y1, x2, ht-y2 )
  }
        
  def calcCord( arr: Array[Double], angle: Double ): (Double, Double, Double, Double, Double, Double) = {

    var arrOut = new Array[Double](6)
    
    val x = arr(0)*math.cos( angle ) - arr(1)*math.sin( angle )
    val y = arr(0)*math.sin( angle ) + arr(1)*math.cos( angle )
    val z = arr(2)
      
    val x0 = arr(0)*math.cos( angle ) - arr(1)*math.sin( angle )
    val y0 = arr(0)*math.sin( angle ) + arr(1)*math.cos( angle )
    val z0 = 0
      
    val xs = (arr(0)+shadowFrac*arr(2))*math.cos( angle ) - arr(1)*math.sin( angle )
    val ys = (arr(0)+shadowFrac*arr(2))*math.sin( angle ) + arr(1)*math.cos( angle )
    val zs = 0
      
    arrOut(0) = y-.5*x
    arrOut(1) = z-.25*x
      
    arrOut(2) = y0-.5*x0
    arrOut(3) = z0-.25*x0
      
    arrOut(4) = ys-.5*xs
    arrOut(5) = zs-.25*xs

    ( arrOut(0), arrOut(1), arrOut(2), arrOut(3), arrOut(4), arrOut(5) )
      
  }
  
  override def paint( g: Graphics) = {	
	  
	if( points!=null ) {
		  
	  var p = points.map( T => calcCord( T.toArray, angle ) ).toArray
		  
	  var xmax = p(0)._1
	  var xmin = p(0)._1
	  var ymax = p(0)._2
	  var ymin = p(0)._2
		  
	  for( i <-0 to p.size-1 ) {

		  if( xmax<p(i)._1 ) xmax = p(i)._1
	  	if( xmax<p(i)._3 ) xmax = p(i)._3
		  if( xmax<p(i)._5 ) xmax = p(i)._5		    

		  if( xmin>p(i)._1 ) xmin = p(i)._1
		  if( xmin>p(i)._3 ) xmin = p(i)._3
		  if( xmin>p(i)._5 ) xmin = p(i)._5		    

		  if( ymax<p(i)._2 ) ymax = p(i)._2
		  if( ymax<p(i)._4 ) ymax = p(i)._4
		  if( ymax<p(i)._6 ) ymax = p(i)._6		    

		  if( ymin>p(i)._2 ) ymin = p(i)._2
		  if( ymin>p(i)._4 ) ymin = p(i)._4
		  if( ymin>p(i)._6 ) ymin = p(i)._6		    
		      
	  }
		  
	  for( i <- 0 to p.size-1 ) {

		  var x_ = (((p(i)._1-xmin)/(xmax-xmin))*(wd-40)+20.5).toInt
	    var y_ = (((p(i)._2-ymin)/(ymax-ymin))*(ht-40)+20.5).toInt
	    var x0 = (((p(i)._3-xmin)/(xmax-xmin))*(wd-40)+20.5).toInt
  	  var y0 = (((p(i)._4-ymin)/(ymax-ymin))*(ht-40)+20.5).toInt  	        
  	  var xs = (((p(i)._5-xmin)/(xmax-xmin))*(wd-40)+20.5).toInt
  	  var ys = (((p(i)._6-ymin)/(ymax-ymin))*(ht-40)+20.5).toInt
  	        
		  g.setColor( Color.black )
		  
		  plotDot( g, x_, y_ )
	    plotLine( g, x_, y_, x0, y0 )
	    g.setColor( Color.gray )
	    plotLine( g, x0, y0, xs, ys )
		    
	  }	  
		  		  
	  if( approxPoints != null ) {
		    
  		var p = approxPoints.map( T => calcCord( T.toArray, angle ) )
			  			    
		for( i <- 0 to p.size-1 ) {
	
		  var x_ = (((p(i)._1-xmin)/(xmax-xmin))*(wd-40)+20.5).toInt
		  var y_ = (((p(i)._2-ymin)/(ymax-ymin))*(ht-40)+20.5).toInt
		  var x0 = (((p(i)._3-xmin)/(xmax-xmin))*(wd-40)+20.5).toInt
	  	  var y0 = (((p(i)._4-ymin)/(ymax-ymin))*(ht-40)+20.5).toInt  	        
	  	  var xs = (((p(i)._5-xmin)/(xmax-xmin))*(wd-40)+20.5).toInt
	  	  var ys = (((p(i)._6-ymin)/(ymax-ymin))*(ht-40)+20.5).toInt
	  	        
		  g.setColor( Color.red )
		  plotDot( g, x_, y_ )
		  plotLine( g, x_, y_, x0, y0 )
		  g.setColor( Color.magenta )
		  plotLine( g, x0, y0, xs, ys )
			    
	    }	  
		    
	  }		  		  

	}
  }
}

class OutputFrame3D( title: String, shadowFrac: Double ) extends Frame( title ) {
  
  val wd = 800
  val ht = 600
  
  def this( title: String ) = this( title, .25 )
  
  var outputCanvas = new OutputCanvas3D( wd, ht, shadowFrac )
  
  def apply() {
    addWindowListener( windowAdapter3D )
    setSize( wd, ht )
    add( "Center", outputCanvas )
    show()
  }
  
  def setData( rdd: RDD[Vector] ) {
    outputCanvas.setData( rdd )
  }
  
  def setAngle( angle: Double ) {
    outputCanvas.angle = angle
  }

  def setApproxPoints( rdd: RDD[Vector] ) {
    outputCanvas.setApproxPoints( rdd )
  }
    
}
