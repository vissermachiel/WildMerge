%inc "&_sasws_/Users/ghermus/_macrodev/wild_merge.sas";

libname test xlsx "&_sasws_/Users/ghermus/_macrodev/reconciliation_data.xlsx" access=readonly;
%wild_merge(inds1=test.crf_data1
           ,inds2=test.ext_data1);
           