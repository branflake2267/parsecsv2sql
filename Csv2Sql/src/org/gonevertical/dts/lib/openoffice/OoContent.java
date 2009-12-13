package org.gonevertical.dts.lib.openoffice;

public class OoContent {

  public OoContent() {
    
  }
  
  public String createContent(String[] tables) {
    String xml = "";
    
    xml += "" +
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?> " +
    "<office:document-content " +
      "xmlns:office=\"urn:oasis:names:tc:opendocument:xmlns:office:1.0\" " +
      "xmlns:style=\"urn:oasis:names:tc:opendocument:xmlns:style:1.0\" " +
      "xmlns:text=\"urn:oasis:names:tc:opendocument:xmlns:text:1.0\" " +
      "xmlns:table=\"urn:oasis:names:tc:opendocument:xmlns:table:1.0\" " +
      "xmlns:draw=\"urn:oasis:names:tc:opendocument:xmlns:drawing:1.0\" " +
      "xmlns:fo=\"urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0\" " +
      "xmlns:xlink=\"http://www.w3.org/1999/xlink\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" " +
      "xmlns:meta=\"urn:oasis:names:tc:opendocument:xmlns:meta:1.0\" " +
      "xmlns:number=\"urn:oasis:names:tc:opendocument:xmlns:datastyle:1.0\" " +
      "xmlns:presentation=\"urn:oasis:names:tc:opendocument:xmlns:presentation:1.0\" " +
      "xmlns:svg=\"urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0\" " +
      "xmlns:chart=\"urn:oasis:names:tc:opendocument:xmlns:chart:1.0\" " +
      "xmlns:dr3d=\"urn:oasis:names:tc:opendocument:xmlns:dr3d:1.0\" xmlns:math=\"http://www.w3.org/1998/Math/MathML\" " +
      "xmlns:form=\"urn:oasis:names:tc:opendocument:xmlns:form:1.0\" " +
      "xmlns:script=\"urn:oasis:names:tc:opendocument:xmlns:script:1.0\" " +
      "xmlns:ooo=\"http://openoffice.org/2004/office\" xmlns:ooow=\"http://openoffice.org/2004/writer\" " +
      "xmlns:oooc=\"http://openoffice.org/2004/calc\" xmlns:dom=\"http://www.w3.org/2001/xml-events\" " +
      "xmlns:xforms=\"http://www.w3.org/2002/xforms\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" " +
      "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:rpt=\"http://openoffice.org/2005/report\" " +
      "xmlns:of=\"urn:oasis:names:tc:opendocument:xmlns:of:1.2\" xmlns:rdfa=\"http://docs.oasis-open.org/opendocument/meta/rdfa#\" " +
      "xmlns:field=\"urn:openoffice:names:experimental:ooo-ms-interop:xmlns:field:1.0\" " +
      "xmlns:formx=\"urn:openoffice:names:experimental:ooxml-odf-interop:xmlns:form:1.0\" " +
      "office:version=\"1.2\"> " +
      "<office:scripts /> " +
      "<office:font-face-decls> " +
        "<style:font-face style:name=\"Nimbus Sans L\" " +
          "svg:font-family=\"&apos;Nimbus Sans L&apos;\" " +
          "style:font-family-generic=\"swiss\" style:font-pitch=\"variable\" /> " +
        "<style:font-face style:name=\"DejaVu Sans\" " +
          "svg:font-family=\"&apos;DejaVu Sans&apos;\" style:font-family-generic=\"system\" " +
          "style:font-pitch=\"variable\" /> " +
      "</office:font-face-decls> " +
      "<office:automatic-styles> " +
        "<style:style style:name=\"co1\" style:family=\"table-column\"> " +
          "<style:table-column-properties " +
            "fo:break-before=\"auto\" style:column-width=\"0.8925in\" /> " +
        "</style:style> " +
        "<style:style style:name=\"ro1\" style:family=\"table-row\"> " +
          "<style:table-row-properties " +
            "style:row-height=\"0.1984in\" fo:break-before=\"auto\" " +
            "style:use-optimal-row-height=\"true\" /> " +
        "</style:style> " +
        "<style:style style:name=\"ta1\" style:family=\"table\" " +
          "style:master-page-name=\"Default\"> " +
          "<style:table-properties table:display=\"true\" " +
            "style:writing-mode=\"lr-tb\" /> " +
        "</style:style> " +
        "<style:style style:name=\"ce1\" style:family=\"table-cell\" " +
          "style:parent-style-name=\"Default\"> " +
          "<style:text-properties fo:font-weight=\"bold\" " +
            "style:font-weight-asian=\"bold\" style:font-weight-complex=\"bold\" /> " +
        "</style:style> " +
        "<style:style style:name=\"ta_extref\" style:family=\"table\"> " +
          "<style:table-properties table:display=\"false\" /> " +
        "</style:style> " +
      "</office:automatic-styles> " +
      "<office:body> " +
        "<office:spreadsheet> ";
        
        for (int i=0; i < tables.length; i++) {
          xml += tables[i];
        }
       
        xml += "</office:spreadsheet> " +
      "</office:body>" +
    "</office:document-content> ";
    
    return xml;
  }
  
}
