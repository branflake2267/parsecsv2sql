package org.gonevertical.dts.v2;

import org.gonevertical.dts.data.SourceData;

public class ProcessImport {

  // data start point
  private SourceData sourceData = null;
  
  // data endpoint
  private DestinationData_v2 dd = null;

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
    this.dd = destinationData;
    fileProcessing = new FileProcessing_v2(sourceData, destinationData);
  }
  
  /**
   * import data
   */
  public void runImport() {
    fileProcessing.run();
    
    if (dd.optimise == true) {
      Optimise_v2 o = new Optimise_v2(dd);
      o.run();
    }
    
    // this will happen on a first import, so to optimise early, then start agian
    if (fileProcessing.getReturnOnOptimise() == true) {
      dd.optimise = false;
      dd.dropTable = false;
      runImport();
    }
  }
  
  /**
   * find data in a csv file
   */
  public void runFindInFile() {
    fileProcessing.findMatchInFile();
  }

  
}
