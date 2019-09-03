# Databio - formats

Long-term goals for databio's format support:

 1. Pluggable formats enable any data interchange format to be supported for both Read and Write operations.
    * Break everything into Records and Fields.
    * Methods to display "location" information to allow debugging data (e.g. line 42, column 9 of CSV versus Cell C42 in Excel)

 2. Generic formats e.g. CSV have support for heuristic header detection.
    * Example heuristic #1: Check the values of the first row to see if the entropy of values is different from others. (e.g. "GeneID" has a wholly different set of characters than "1234")
    * Example heuristic #2: scan N rows and compare entropy of columns to look for a breakpoint. (e.g. multiple-line headers)

3. Database of headers to allow known, popular format types to be quickly detected without heuristics.