<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="html"/>
<xsl:template match="configuration">
<table border="1" align="center" >
<tr>
 <th>name</th>
 <th>value</th>
</tr>
<xsl:for-each select="property">
<tr>
  <td width="35%"><b><xsl:value-of select="name"/></b></td>
  <td width="65%"><xsl:value-of select="value"/></td>
</tr>
</xsl:for-each>
</table>
</xsl:template>
</xsl:stylesheet>
