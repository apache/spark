/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.mapred.JSPUtil.JobWithViewAccessCheck;
import org.apache.hadoop.security.UserGroupInformation;

/** The servlet that outputs svg graphics for map / reduce task
 *  statuses
 */
public class TaskGraphServlet extends HttpServlet {

  private static final long serialVersionUID = -1365683739392460020L;

  /**height of the graph w/o margins*/ 
  public static final int width = 600;
  
  /**height of the graph w/o margins*/ 
  public static final int height = 200;
  
  /**margin space on y axis */
  public static final int ymargin = 20;

  /**margin space on x axis */
  public static final int xmargin = 80;
  
  private static final float oneThird = 1f / 3f;
  
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {

    response.setContentType("image/svg+xml");

    final JobTracker tracker = 
      (JobTracker) getServletContext().getAttribute("job.tracker");
    
    String jobIdStr = request.getParameter("jobid");
    if(jobIdStr == null)
      return;
    final JobID jobId = JobID.forName(jobIdStr);

    // verify if user has view access for this job
    JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(
        tracker, jobId, request, response);
    if (!myJob.isViewJobAllowed()) {
      return;// user is not authorized to view this job
    }

    final boolean isMap = "map".equalsIgnoreCase(request.getParameter("type"));
    final TaskReport[] reports = isMap? tracker.getMapTaskReports(jobId) 
                                      : tracker.getReduceTaskReports(jobId);
    if(reports == null || reports.length == 0) {
      return;
    }

    final int numTasks = reports.length;     
    int tasksPerBar = (int)Math.ceil(numTasks / 600d);
    int numBars = (int) Math.ceil((double)numTasks / tasksPerBar);
    int w = Math.max(600, numBars);
    int barWidth = Math.min(10,  w / numBars); //min 1px, max 10px
    int barsPerNotch = (int)Math.ceil(10d / barWidth);
    w = w + numBars / barsPerNotch;
    int totalWidth = w + 2 * xmargin;
    
    //draw a white rectangle
    final PrintWriter out = response.getWriter();
    out.print("<?xml version=\"1.0\" standalone=\"no\"?>\n" + 
      "<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \n" + 
      "\"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\">\n" +
      "<?xml-stylesheet type=\"text/css\" href=\"/static/hadoop.css\"?>\n\n"+
      "<svg width=\"");out.print(totalWidth);
    out.print("\" height=\"");out.print(height + 2 * ymargin);
    out.print("\" version=\"1.1\"\n" + 
      "xmlns=\"http://www.w3.org/2000/svg\">\n\n"); 
    
    //axes
    printLine(out, xmargin - 1, xmargin - 1, height + ymargin + 1
        , ymargin - 1, "black" );
    printLine(out, xmargin - 1, w + xmargin + 1 ,height + ymargin + 1 
        , height + ymargin + 1, "black" );
    
    //borderlines
    printLine(out, w + xmargin + 1 , w + xmargin +1
        , height + ymargin + 1,ymargin - 1, "#CCCCCC" );
    printLine(out, xmargin - 1, w + xmargin + 1
        , ymargin - 1 , ymargin - 1, "#CCCCCC" );
    
    String[]  colors = new String[] {"#00DD00", "#E50000", "#AAAAFF"};
    
    //determine the notch interval using the number of digits for numTasks
    int xNotchInterval = (int)(Math.ceil( numTasks / 10d));
    
    int xOffset = -1; 
    int xNotchCount = 0;
    //task bar graph
    for(int i=0, barCnt=0; ;i+=tasksPerBar, barCnt++) {
      if(barCnt % barsPerNotch == 0) {
        xOffset++;
      }
      int x = barCnt * barWidth + xmargin + xOffset;
      //x axis notches
      if(i >= xNotchInterval * xNotchCount) {
        printLine(out, x, x, height + ymargin + 3 
            , height + ymargin - 2, "black");
        printText(out, x, height + ymargin + 15 
            , String.valueOf(xNotchInterval * xNotchCount++ ), "middle");
      }
      if(i >= reports.length) break;
      
      if(isMap) {
        float progress = getMapAvarageProgress(tasksPerBar, i, reports);
        int barHeight = (int)Math.ceil(height * progress);
        int y = height - barHeight + ymargin;
        printRect(out, barWidth, barHeight,x , y , colors[2]);
      }
      else {
        float[] progresses 
          = getReduceAvarageProgresses(tasksPerBar, i, reports);
        //draw three bars stacked, for copy, sort, reduce
        
        int prevHeight =0;
        for(int j=0; j < 3 ; j++) {
          int barHeight = (int)((height / 3) * progresses[j]);
          if(barHeight > height/ 3 - 3)//fix rounding error
            barHeight = height / 3 + 1;
          
          int y = height - barHeight + ymargin - prevHeight;
          prevHeight += barHeight;
          printRect(out, barWidth, barHeight, x, y, colors[j] );
        }
      }
    }
    
    //y axis notches
    for(int i=0;i<=10;i++) {
      printLine(out, xmargin-3 , xmargin+2 , ymargin + (i * height) / 10
          , ymargin + (i * height) / 10 , "black");
      printText(out, xmargin - 10 , ymargin + 4 + (i * height) / 10 
          , String.valueOf(100 - i * 10), "end");
    }
    
    if(!isMap) {
      //print color codes for copy, sort, reduce
      printRect(out, 14, 14, xmargin + w + 4, ymargin + 20, colors[0]);
      printText(out, xmargin + w + 24, ymargin + 30, "copy", "start");
      printRect(out, 14, 14, xmargin + w + 4, ymargin + 50, colors[1]);
      printText(out, xmargin + w + 24, ymargin + 60, "sort", "start");
      printRect(out, 14, 14, xmargin + w + 4, ymargin + 80, colors[2]);
      printText(out, xmargin + w + 24, ymargin + 90, "reduce", "start");
    }
    
    
    //firefox curently does not support vertical text
    //out.print("<text x=\"");out.print(6);
    //out.print("\" y=\""); out.print(ymargin + height / 2); 
    //out.print("\" style=\"text-anchor:middle;writing-mode:tb\">"
    //+"Percent</text>\n");
    
    out.print("</svg>");
  }

  /**Computes average progress per bar*/
  private float getMapAvarageProgress(int tasksPerBar, int index
      , TaskReport[] reports ) {
    float progress = 0f;
    int k=0;
    for(;k < tasksPerBar && index + k < reports.length; k++) { 
      progress += reports[index + k].getProgress();
    }
    progress /= k;
    return progress;
  }

  /**Computes average progresses per bar*/
  private float[] getReduceAvarageProgresses(int tasksPerBar, int index
      , TaskReport[] reports ) {
    float[] progresses = new float[] {0,0,0};
    int k=0;
    for(;k < tasksPerBar && index + k < reports.length; k++) {
      float progress = reports[index+k].getProgress();
      for(int j=0; progress > 0 ; j++, progress -= oneThird) {
        if(progress > oneThird)
          progresses[j] += 1f;
        else 
          progresses[j] += progress * 3 ;
      }
    }
    for(int j=0; j<3; j++) { progresses[j] /= k;}
    
    return progresses;
  }
  
  private void printRect(PrintWriter out, int width, int height
      , int x, int y, String color) throws IOException {
    if(height > 0) {
      out.print("<rect width=\"");out.print(width);
      out.print("\" height=\"");  out.print(height);
      out.print("\" x=\""); out.print(x);
      out.print("\" y=\""); out.print(y);
      out.print("\" style=\"fill:"); out.print(color);out.print("\"/>\n");
    }
  }
  private void printLine(PrintWriter out, int x1, int x2
      , int y1, int y2, String color) throws IOException {
    out.print("<line x1=\"");out.print(x1);
    out.print("\" x2=\"");out.print(x2);
    out.print("\" y1=\"");out.print(y1);
    out.print("\" y2=\""); out.print(y2);
    out.print("\" class=\"taskgraphline\" style=\"stroke:"); 
    out.print(color); out.print("\"/>\n"); 
  }
  private void printText(PrintWriter out, int x, int y, String text
      , String anchor) throws IOException {
    out.print("<text x=\"");out.print(String.valueOf(x));
    out.print("\" y=\""); out.print(String.valueOf(y));
    out.print("\" style=\"fill:black;font-family:sans-serif;" 
        + "text-anchor:");out.print(anchor); out.print("\">");
    out.print(text); out.print("</text>\n");
  }
}

