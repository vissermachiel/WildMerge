%************************************************************************
                                                                         
 Program Name    : wild_merge                                            
 Purpose         : Perform a wild-key merge to reconcile datasets        
 Author          : Gerard Hermus                                         
 Created on      : SAS 9.4                                               
 Disclaimer      : This macro comes with no warranty or guarantee. 
                   The code is imperfect, poorly commented, not optimized, it has 
                   not been formally validated and more than likely contains 
                   lamentable inexactitudes.
*************************************************************************
 Revision History                                                        
                                                                         
 Author          :                                                       
 Date            :                                                       
 Revision        :                                                       
                                                                         
************************************************************************;

%macro wild_merge(inds1=
                 ,inds2=
                 ,imp_key=STUDYID USUBJID SUBJID SUBJECT
                 ,key=
                 ,perm_key=
                 ,limit=200
                 ,debug=N);
                 
                 
   %* inds1    = Required, specifies the main input dataset. E.g. EDC lab sample dates.
                 This data will not be allowed to have duplicate entries
                 
      inds2    = Required, specifies the secundairy input dataset. E.g. external lab results
      
      imp_key  = Not required, specifies a default set of variables which you always want in your key
                 Ignored when the key parameter is specified.
      
      key      = Not required, but allows the user to manually specify which variables have to be used for the merge
      
      perm_key = Not required, allows the user to manually specify which variables are to be permissably used in the merge
                 e.g. VISIT LBDTC LBREFID 
                 
                 will make the macro create a list of all possible key combinations using the above input
                 
                 VISIT LBDTC LBREFID
                 VISIT LBDTC
                 VISIT LBREFID
                 LBDTC LBREFID
                 VISIT 
                 LBDTC 
                 LBREFID
                 
                 If any entries are made in the parameter key, the above will be combined with that input. E.g. if we have key=USUBJID it will create:
                 
                 USUBJID VISIT LBDTC LBREFID
                 USUBJID VISIT LBDTC
                 USUBJID VISIT LBREFID
                 USUBJID LBDTC LBREFID
                 USUBJID VISIT 
                 USUBJID LBDTC 
                 USUBJID LBREFID
                 USUBJID
                 
                 This macro still contains a small extra 
      
      limit    = Not required, puts a hard limit on the amount of merge attempts. 
                 If there are more merges available than the limit, the macro will stop.
                 
      debug    = Not required, Y/N variable, defaults to N. Switches on the debug mode of the macro which stops it from dropping all datasets;
   
   proc datasets lib=work nolist nowarn;
      delete _wm_t:;
   run;

   %let inds1=%upcase(&inds1);
   %let inds2=%upcase(&inds2);
   %let debug=%upcase(&debug);
   
   %let key     =%upcase(%sysfunc(compbl(%str(&key ))));
   %let perm_key=%upcase(%sysfunc(compbl(%str(&perm_key ))));
   %let imp_key =%sysfunc(tranwrd(%upcase(%sysfunc(compbl(%str("&imp_key")))),%str( ),%str(" ")));
   
   %* standardize ds1/ds2 name;
   %* create unique id;
   data _wm_t_ds1_0;
      set &inds1 end=end;
      ds1_unid=_n_;
      delete=0;
      if end then call symputx('ds1_unid_ntot',ds1_unid);
   run;
   
   data _wm_t_ds2_0;
      set &inds2 end=end;
      ds2_unid=_n_;
      delete=0;
      if end then call symputx('ds2_unid_ntot',ds2_unid);
   run;
   
   %put INFO: WILD_MERGE: &inds1 has &ds1_unid_ntot records to merge;
   %put INFO: WILD_MERGE: &inds2 has &ds2_unid_ntot records to merge;
   
   %* could add SDTM recognition with XXDTC renaming to DTC for automatic in between domain alignments;
   
   %* retrieve metadata;
   proc sql noprint;
      create table _wm_t_meta1 (where=(name not in ("DS1_UNID" "DS2_UNID" "DELETE"))) as 
         select memname,upcase(name) as name,min(varnum) as varnum,type
         from dictionary.columns
         where libname="WORK" and memname in ("_WM_T_DS1_0" "_WM_T_DS2_0")
         group by name
         order by name,varnum;
   quit;
   
   %* transpose;
   proc transpose data= _wm_t_meta1 out= _wm_t_meta2;
      by name varnum;
      var type;
      id memname;
   run;
   
   proc sort;
      by varnum;
   run;
   
   %* based on availability of metadata, determine variables in common;
   data _wm_t_meta3;
      set _wm_t_meta2 end=end;
      by varnum;
      length auto_key auto_perm_key $400;
      retain auto_key auto_perm_key "";
      
      if _wm_t_ds1_0=_wm_t_ds2_0 then do;
         if name in (&imp_key)  then auto_key=strip(auto_key)||" "||strip(name);
         else  auto_perm_key=strip(auto_perm_key)||" "||strip(name);
      end;
      
      if end and "&key"="" and "&perm_key"="" then call symput("key",strip(auto_key));
      if end and "&perm_key"="" then call symput("perm_key",strip(auto_perm_key));
   run;
   
   %* create an all key mvar;
   %local all_key;
   %let all_key=%sysfunc(tranwrd(%sysfunc(compbl(&key &perm_key)),%str(|),%str( )));
   
   
   %* start empty permutable key variable dataset;
   data _wm_t_perm_key_1;
      length key $200;   
      key="&key";
      length it 8. perm_key $200;
      it=1;
      perm_key=" &perm_key ";
   run;
   
   %* set initial stop sign to 0;
   %let recon_stop_perm=0;
   
   %* count iterations;
   %let it1=0;
   
   %* loop through permutations until no variables are left to remove;
   %do %until(&recon_stop_perm=1);
   
      %let it1=%eval(&it1+1);
      
      data _wm_t_perm_keydrop (drop=perm_key rename=(perm_key2=perm_key));
         set _wm_t_perm_key_&it1;
         it=it+1; 
         length perm_key2  $200;   
         perm_key2=perm_key;
         do i = 1 to  countw(tranwrd(perm_key,"|","_"));
            perm_key2=compbl(tranwrd(perm_key," "||scan(perm_key,i," ")||" "," "));
            if compress(perm_key2) ne "" then output;
         end;
      run;
      
      proc sql noprint;
         create table _wm_t_perm_key_%eval(&it1+1) as 
            select distinct key,it,perm_key
            from _wm_t_perm_keydrop;
      quit;
      
      %if &sqlobs=0 %then %let recon_stop_perm=1;
   %end;

   %* combine all permutations;
   data _wm_t_perm_key1;
      set _wm_t_perm_key_:;
      if perm_key="" then delete;
   run;

   data _wm_t_ds3_0;
      set _wm_t_ds1_0 (keep=&all_key)
          _wm_t_ds2_0 (keep=&all_key);
   run;
      
   %* remove intermediate steps;
   proc datasets lib=work nowarn nolist;
      delete _wm_t_perm_key_: _wm_t_perm_keydrop;
   run;
   
   %* per permutation, determine the amount of unique records in the DS1;
   %* the unique records translates to a unique_rec score. 
      The higher the number the more unique_rec/explanatory power the key combination has;
   data _null;
      file "%sysfunc(pathname(work))/unique_rec1.sas";
      set _wm_t_perm_key1;
      
      length key_sql key_sas  $200;
      if key ne "" then do;
         key_sql=translate(strip(key),",,","| ")||",";
         key_sas=tranwrd(key_sql,","," ");
      end;
      else call missing(key_sql,key_sas);
      
      length perm_key_sql perm_key_sas  $200;
      perm_key_sql=translate(strip(perm_key),",,","| ");
      perm_key_sas=tranwrd(perm_key_sql,","," ");

      put 'proc sql noprint;';
      put '  create table _wm_t_key_val_' _n_ z8. ' as ';
      put '  select distinct ' key_sql +(-1) perm_key_sql +(-1) ;
      put '  from _wm_t_ds3_0 (keep=' key_sas +(-1) ' ' perm_key_sas +(-1) ');';
      put 'quit;';
      
      put 'data _wm_t_key_spec_' _n_ z8. ';';
      put '   length it 8. perm_key $200;';
      put '   it=' it 8. ';';
      put '   perm_key="' perm_key_sas +(-1) '";';
      put '   unique_rec= &sqlobs;';
      put 'run;';
   run;
   
   %inc "%sysfunc(pathname(work))/unique_rec1.sas";
   
   %if "&key" ne "" %then %do; 
      data _wm_t_key_spec_0;
         length it 8. perm_key $200 unique_rec 8.;
         call missing(it,perm_key,unique_rec);
      run;
   %end;
   
   %* combine all permutations;
   data _wm_t_key_spec1;
      set _wm_t_key_spec_:;
   run;
   
   %* remove all intermediate steps;
   proc datasets lib=work nowarn nolist;
      delete _wm_t_key_spec_:;
   run;
   
   proc sql noprint;
      select distinct nobs into: recon_n_key
      from dictionary.tables
      where libname="WORK" and memname="_WM_T_KEY_SPEC1";
   quit;
   
   %if &recon_n_key>&limit %then %do;
      %put ERRO%str(R:) Macro has been aborted due to the combination of keys being larger than the set limit: &limit;
      %goto endmac_wild_merge;
   %end;
   
   %* sort by unique_rec, highest needs to be first;
   proc sort data=_wm_t_key_spec1;
      by descending unique_rec it perm_key;
   run;
   
   %* datastep which creates a macro containing all different merges;
   %* The merge is performed on datasets with just the selected key in, and the unique record identifier;
   %* - Helps reducing resource usage;
   %* - in the end leads to a dataset which contains a merged list unique ds1/ds2 identifiers;
   %*   and is later on used to retrieve the rest of the data;
   data _wm_t_key_spec2;
      file "%sysfunc(pathname(work))/merges1.sas";
      set _wm_t_key_spec1 end=end;

      if "&key" ne "" and perm_key ne "" then perm_key_sql=translate(compbl("&key "||strip(perm_key)),","," ");
      else if "&key" ne "" and perm_key="" then perm_key_sql=translate(compbl("&key"),","," ");
      else perm_key_sql=translate(strip(perm_key),",,"," ");
      
      perm_key_sas=tranwrd(perm_key_sql,","," ");
      
      mergen    =strip(put(_n_,best32.));
      mergen_prev=strip(put(_n_-1,best32.));
      
      %* start temporary macro;
      if _n_=1 then put '%macro wm_temp;';
      
      %* per key create dataset for DS1 and DS2 that can  be used to merge.
       %* calculate whether there are duplicate records within each;
      do i = 1 to 2;
         put 'proc sql noprint;';
         put '  create table _wm_t_ds' i 1. '_' mergen '(where=(delete ne 1)) as ';
         put '  select ' perm_key_sql +(-1) ', a.ds' i 1. '_unid, b.delete, count(distinct a.ds' i 1. '_unid) as ds' i 1. '_dup';
         put '  from _wm_t_ds' i 1. '_0 (drop=delete) as a';
         %* only use previous dataset if its not the first merge;
         if _n_ ne 1 then do;
               put '  left join (select distinct ds' i 1. '_unid,1 as delete';
               put '            from _wm_t_ds3t_total) as b on a.ds' i 1. '_unid=b.ds' i 1. '_unid';
         end;
         else put ',(select distinct 0 as delete from _wm_t_key_spec1) as b';
         put '  group by  ' perm_key_sql +(-1) ';';
         put 'quit;';
      end;
      
      %* perform the merge;
      put 'proc sql noprint;';
      put '  create table _wm_t_ds3_' mergen ' as ';
      put '  select ds1_unid,ds2_unid,ds1_dup,ds2_dup';
      put '  from                    _wm_t_ds1_' mergen;
      put '       natural inner join _wm_t_ds2_' mergen ';';
      put 'quit;';
   
      %* create overview of all merges;
      put 'data _wm_t_ds3t_total;';
      put '   set';
      %* only use previous dataset if its not the first merge;
      if _n_ ne 1 then put '_wm_t_ds3t_total';
      put '       _wm_t_ds3_' mergen '(where=(ds1_dup=1 and ds2_dup>=1));';
      
      put 'run;';
      
      %* Count distinct unique records that have been merged;
      put 'proc sql noprint;';
      put '   select count(distinct ds1_unid),count(distinct ds2_unid) into: ds1_unid_n,:ds2_unid_n';
      put '   from _wm_t_ds3t_total;';
      put 'quit;';
      
      %* Exit the merges when one of the sides had all records merged;
      %* This is done to limit resource usage;
      put '%if &ds1_unid_n=&ds1_unid_ntot or &ds2_unid_n=&ds2_unid_ntot %then %do;';
      put '   %goto wm_temp_done;';
      put '%end;';
           
      if end=1 then do;
         %* exit clause when all records have been merged;
         put '   %wm_temp_done:';
         put '   %put INFO: WILD_MERGE: Merge completed with DS1 &ds1_unid_n/&ds1_unid_ntot and DS2 &ds2_unid_n/&ds2_unid_ntot;';
         %* end temporary macro;
         put '%mend wm_temp;';
         %*run;
         put '%wm_temp;';
      end;
   run;
   
   %inc "%sysfunc(pathname(work))/merges1.sas";
   
   %* rename all the input key variables for ds1;
   data _null_;
      file "%sysfunc(pathname(work))/rename_key_vars_ds1.sas";
      set _wm_t_meta2 end=end;
      if _n_=1 then do;
         put 'proc sql noprint;';
         put '  create table _wm_t_ds1f_final as ';
         put '  select ds1_unid' ;
      end;
      if _wm_t_ds1_0 ne "" and _wm_t_ds2_0 ne "" then put ',' name ' as ds1_' name +(-1);
      else if _wm_t_ds1_0 ne "" then put ',' name ;
      if end then do;
         put '   from _wm_t_ds1_0';
         put ' order by ds1_unid;';
         put 'quit;';
      end;
   run;
   
   %inc "%sysfunc(pathname(work))/rename_key_vars_ds1.sas";
   
   %* rename all the input key variables for ds2;
   %* also merge on the ds1_unid to allow merging on that;
   data _null_;
      file "%sysfunc(pathname(work))/rename_key_vars_ds2.sas";
      set _wm_t_meta2 end=end;
      if _n_=1 then do;
         put 'proc sql noprint;';
         put '  create table _wm_t_ds2f_final as ';
         put '  select b.ds1_unid,a.ds2_unid,ds2_dup' ;
      end;
      if _wm_t_ds1_0 ne "" and _wm_t_ds2_0 ne "" then put ',' name ' as ds2_' name +(-1);
      else if _wm_t_ds2_0 ne "" then put ',' name;
      if end then do;
         put 'from           _wm_t_ds2_0 as a 
                 left join _wm_t_ds3t_total as b on a.ds2_unid=b.ds2_unid';
         put ' order by b.ds1_unid;';
         put 'quit;';
      end;
   run;
      
   %inc "%sysfunc(pathname(work))/rename_key_vars_ds2.sas";
   
   %if "&debug"="N" %then %do;
      proc datasets lib=work nowarn nolist;
         delete _wm_t_ds1_: _wm_t_ds2_: _wm_t_ds3_:;
      run;
   %end;
   
   data _wm_t_output;
      length check &all_key WM_%sysfunc(tranwrd(&all_key,%str( ),%str( WM_))) $200;
      merge   _wm_t_ds1f_final (in=_wm_ds1) 
              _wm_t_ds2f_final (in=_wm_ds2);
      by ds1_unid;
      
      _wm_sort=coalesce(ds1_unid,_n_);
      
           if _wm_ds1>_wm_ds2               then check="Record is only in &inds1";
      else if _wm_ds1<_wm_ds2               then check="Record is only in &inds2"; 
      else if _wm_ds1=_wm_ds2 and ds2_dup>1 then check="Record was in both datasets, but &inds2 was not unique"; 

      %do _rec_itt=1 %to %sysfunc(countw(&all_key));
         %local _wm_var;
         %let _wm_var=%scan(&all_key,&_rec_itt,%str( ));
         
         if  _wm_ds1>_wm_ds2 then do;
            wm_&_wm_var=strip(vvalue(ds1_&_wm_var));
            &_wm_var=wm_&_wm_var;
         end;
         else if  _wm_ds1<_wm_ds2 then do;
            wm_&_wm_var=strip(vvalue(ds2_&_wm_var));
            &_wm_var=wm_&_wm_var;
         end;
         else if ds1_&_wm_var=ds2_&_wm_var then do;
            wm_&_wm_var=coalescec(strip(vvalue(ds1_&_wm_var)),strip(vvalue(ds2_&_wm_var)));
            &_wm_var=wm_&_wm_var;
         end;
         else if ds1_&_wm_var ne ds2_&_wm_var then do;
            check=strip(check)||"| &_wm_var is unequal";
            wm_&_wm_var="&inds1: "||strip(vvalue(ds1_&_wm_var))||" | &inds2: "||strip(vvalue(ds2_&_wm_var));
            &_wm_var=strip(vvalue(ds1_&_wm_var));
         end; 
      %end;
   run;
   
   proc sort data=_wm_t_output out=_wm_recon (keep=check wm_:) nodupkey;
      by &all_key check wm_:;
   run;
   
   proc sort data=_wm_t_output out=_wm_merge (drop=_wm_sort wm_: ds1_: ds2_:);
      by &all_key;
   run;
   
   %if "&debug"="N" %then %do;
      proc datasets lib=work nowarn nolist;
         delete _wm_t_:;
      run;
   %end;
   
   %endmac_wild_merge:


%mend wild_merge;