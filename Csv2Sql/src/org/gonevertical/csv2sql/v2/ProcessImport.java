package org.gonevertical.csv2sql.v2;

import org.gonevertical.csv2sql.data.SourceData;

public class ProcessImport {

  // data start point
  private SourceData sourceData = null;
  
  // data endpoint
  private DestinationData_v2 destinationData = null;

  // file processing class - starts the process going through the files
  private FileProcessing_v2 fileProcessing = null;
  
  /**
   * constructor - init
   * 
   * @param sourceData
   * @param destinationData
   */
  public ProcessImport(SourceData sourceData, DestinationData_v2 destinationData) {
    this.sourceData = sourceData;
    this.destinationData = destinationData;
    fileProcessing = new FileProcessing_v2(sourceData, destinationData);
  }
  
  /**
   * import data
   */
  public void runImport() {
    fileProcessing.run();
    
    if (destinationData.optimise == true) {
      Optimise_v2 o = new Optimise_v2(destinationData);
      o.run();
    }
  }
  
  /**
   * find data in a csv file
   */
  public void runFindInFile() {
    fileProcessing.findMatchInFile();
  }
  
}