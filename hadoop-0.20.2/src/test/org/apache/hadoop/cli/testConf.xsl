<?xml version="1.0" encoding="ISO-8859-1"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  
  <xsl:template match="/">
    <html>
      <body>
        <h2>Hadoop DFS command-line tests</h2>
        <table border="1">
          <tr bgcolor="#9acd32">
            <th align="left">ID</th>
            <th align="left">Command</th>
            <th align="left">Description</th>
          </tr>
          <xsl:for-each select="configuration/tests/test">
            <!-- <xsl:sort select="description"/> -->
            <tr>
              <td><xsl:value-of select="position()"/></td>
              <td><xsl:value-of select="substring-before(description,':')"/></td>
              <td><xsl:value-of select="substring-after(description,':')"/></td>
            </tr>
          </xsl:for-each>
        </table>
      </body>
    </html>
  </xsl:template>
  
</xsl:stylesheet>
