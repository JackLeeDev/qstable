#ifndef _qdef_h_
#define _qdef_h_

#define qstrdup strdup
#define safe_free(p) if(p){free((void*)p);p=NULL;}

#endif