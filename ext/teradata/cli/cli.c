/*
 *  $Id: cli.c 629 2010-02-17 01:46:11Z aamine $
 *
 *  Copyright (C) 2009,2010 Teradata Japan, LTD.
 *
 *  This program is free software.
 *  You can distribute/modify this program under the terms of
 *  the GNU LGPL2, Lesser General Public License version 2.
 */

#include <ruby.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>

#include <coptypes.h>
#include <coperr.h>
#include <dbcarea.h>
#include <parcel.h>

static VALUE CLI;
static VALUE CLIError;

struct rb_cli {
  VALUE initialized;    // bool
  VALUE logging_on;     // bool
  char session_charset[36];
#define CHARSET_BUFSIZE 32
  struct DBCAREA dbcarea;
};

static VALUE cli_initialized = Qfalse;

static struct rb_cli * get_cli(VALUE self);
static void cli_free(struct rb_cli *p);
static void logon(struct rb_cli *p, char *logon_string);
static void logoff(struct rb_cli *p, VALUE force);
static void dispatch(struct rb_cli *p, Int32 func);
static char* status_name(Int32 status);
static char* flavor_name(Int32 flavor);

  static struct rb_cli *
check_cli(VALUE self)
{
  Check_Type(self, T_DATA);
  if (RDATA(self)->dfree != (RUBY_DATA_FUNC)cli_free) {
    rb_raise(rb_eTypeError, "wrong argument type %s (expected CLI)",
        rb_class2name(CLASS_OF(self)));
  }
  return DATA_PTR(self);
}

  static struct rb_cli *
get_cli(VALUE self)
{
  struct rb_cli *p = check_cli(self);
  if (!p || !p->initialized) {
    rb_raise(rb_eRuntimeError, "uninitialized CLI object");
  }
  return p;
}

  static VALUE
cli_s_allocate(VALUE klass)
{
  struct rb_cli* p;

  p = ALLOC_N(struct rb_cli, 1);
  p->dbcarea.total_len = sizeof(struct DBCAREA);
  p->initialized = Qfalse;
  p->logging_on = Qfalse;
  return Data_Wrap_Struct(klass, NULL, cli_free, p);
}

  static void
cli_free(struct rb_cli *p)
{
  if (p->logging_on) {
    logoff(p, Qtrue);
  }
}

  static VALUE
cli_initialize(VALUE self, VALUE logon_string, VALUE session_charset)
{
  struct rb_cli *p;
  Int32 status;
  char dummy[4] = {0,0,0,0};

  p = check_cli(self);
  if (p->initialized) {
    rb_raise(rb_eRuntimeError, "already initialized CLI object");
  }

  StringValue(logon_string);
  StringValue(session_charset);

  DBCHINI(&status, dummy, &p->dbcarea);
  if (status != EM_OK) {
    rb_raise(CLIError, "CLI error: [%s] %s",
        status_name(status), p->dbcarea.msg_text);
  }
  cli_initialized = Qtrue;

  p->dbcarea.change_opts = 'Y';
  p->dbcarea.wait_for_resp = 'Y';    // Complete response and return.
  p->dbcarea.keep_resp = 'N';        // We do not rewind.
  p->dbcarea.wait_across_crash = 'Y';  // CLI returns when DBC is not available.
  p->dbcarea.tell_about_crash = 'Y';
  p->dbcarea.use_presence_bits = 'N'; // We do not send data by record
  p->dbcarea.var_len_req = 'N';
  p->dbcarea.var_len_fetch = 'N';
  p->dbcarea.loc_mode = 'Y';         // Locate mode (not move mode)
  p->dbcarea.parcel_mode = 'Y';
  p->dbcarea.save_resp_buf = 'N';    // free response buffer
  p->dbcarea.two_resp_bufs = 'N';    // disable double buffering
  p->dbcarea.ret_time = 'N';
  p->dbcarea.resp_mode = 'I';        // Indicator mode
  p->dbcarea.req_proc_opt = 'B';     // process request and return response,
  // with column names and EXPLAIN data.

  p->dbcarea.charset_type = 'N';     // multibyte character set
  snprintf(p->session_charset, CHARSET_BUFSIZE,
      "%-30s", StringValueCStr(session_charset));
  p->dbcarea.inter_ptr = p->session_charset;

  logon(p, StringValueCStr(logon_string));
  p->initialized = Qtrue;

  return Qnil;
}

  static void
logon(struct rb_cli *p, char *logon_string)
{
  if (p->logging_on) {
    rb_raise(CLIError, "already logged on");
  }
  p->dbcarea.logon_ptr = logon_string;
  p->dbcarea.logon_len = strlen(logon_string);
  dispatch(p, DBFCON);
  p->dbcarea.i_sess_id = p->dbcarea.o_sess_id;
  p->dbcarea.i_req_id = p->dbcarea.o_req_id;
  p->logging_on = Qtrue;
}

  static VALUE
cli_logoff(VALUE self)
{
  struct rb_cli *p = get_cli(self);
  if (! p->logging_on) {
    rb_raise(CLIError, "is already logged off");
  }
  logoff(p, Qfalse);
  return Qnil;
}

  static void
logoff(struct rb_cli *p, VALUE force)
{
  Int32 status;
  char dummy[4] = {0, 0, 0, 0};

  p->dbcarea.func = DBFDSC;
  DBCHCL(&status, dummy, &p->dbcarea);
  if (!force && status != EM_OK) {
    rb_raise(CLIError, "CLI error: [%s] %s",
        status_name(status), p->dbcarea.msg_text);
  }
  p->logging_on = Qfalse;
}

  static VALUE
cli_logging_on_p(VALUE self)
{
  struct rb_cli *p = get_cli(self);
  return p->logging_on;
}

/* must be called only once in process, at the finalize step. */
  static VALUE
cli_cleanup(VALUE mod)
{
  Int32 status;
  char dummy[4];

  DBCHCLN(&status, dummy);
  if (status != EM_OK) {
    rb_raise(CLIError, "CLI cleanup failed");
  }
  return Qnil;
}

  static VALUE
cli_session_charset(VALUE self)
{
  struct rb_cli *p = get_cli(self);
  return rb_str_new2(p->session_charset);
}

  static VALUE
cli_send_request(VALUE self, VALUE sql_value)
{
  struct rb_cli *p = get_cli(self);
  StringValue(sql_value);
  p->dbcarea.req_ptr = RSTRING_PTR(sql_value);
  p->dbcarea.req_len = RSTRING_LEN(sql_value);
  dispatch(p, DBFIRQ);
  p->dbcarea.i_sess_id = p->dbcarea.o_sess_id;
  p->dbcarea.i_req_id = p->dbcarea.o_req_id;
  return Qnil;
}

  static VALUE
cli_end_request(VALUE self)
{
  struct rb_cli *p = get_cli(self);
  dispatch(p, DBFERQ);
  return Qnil;
}

  static VALUE
cli_fetch(VALUE self)
{
  struct rb_cli *p = get_cli(self);
  dispatch(p, DBFFET);
  return Qnil;
}

  static VALUE
cli_message(VALUE self)
{
  struct rb_cli *p = get_cli(self);
  return rb_str_new2(p->dbcarea.msg_text);
}

  static VALUE
cli_data(VALUE self)
{
  struct rb_cli *p = get_cli(self);
  return rb_str_new(p->dbcarea.fet_data_ptr, p->dbcarea.fet_ret_data_len);
}

  static void
dispatch(struct rb_cli *p, Int32 func)
{
  Int32 status;
  char dummy[4] = {0, 0, 0, 0};

  p->dbcarea.func = func;
  DBCHCL(&status, dummy, &p->dbcarea);
  if (status != EM_OK) {
    rb_raise(CLIError, "CLI error: [%s] %s",
        status_name(status), p->dbcarea.msg_text);
  }
}

  static char*
status_name(Int32 status)
{
  switch (status) {
    case EM_OK: return "EM_OK";
    case EM_BUFSIZE: return "EM_BUFSIZE";
    case EM_FUNC: return "EM_FUNC";
    case EM_NETCONN: return "EM_NETCONN";
    case EM_NOTIDLE: return "EM_NOTIDLE";
    case EM_REQID: return "EM_REQID";
    case EM_NOTACTIVE: return "EM_NOTACTIVE";
    case EM_NODATA: return "EM_NODATA";
    case EM_DATAHERE: return "EM_DATAHERE";
    case EM_ERRPARCEL: return "EM_ERRPARCEL";
    case EM_CONNECT: return "EM_CONNECT";
    case EM_BUFOVERFLOW: return "EM_BUFOVERFLOW";
    case EM_TIMEOUT: return "EM_TIMEOUT";
    case EM_BREAK: return "EM_BREAK";
    case SESSOVER: return "SESSOVER";
    case NOREQUEST: return "NOREQUEST";
    case BADPARCEL: return "BADPARCEL";
    case REQEXHAUST: return "REQEXHAUST";
    case BUFOVFLOW: return "BUFOVFLOW";
    default:
                    {
                      static char buf[64];
                      sprintf(buf, "EM_%d", status);
                      return buf;
                    }
  }
}

  static VALUE
cli_flavor_name(VALUE self)
{
  struct rb_cli *p = get_cli(self);
  return rb_str_new2(flavor_name(p->dbcarea.fet_parcel_flavor));
}

  static char*
flavor_name(Int32 flavor)
{
  switch (flavor) {
    case PclREQ: return "PclREQ";
    case PclRUNSTARTUP: return "PclRUNSTARTUP";
    case PclDATA: return "PclDATA";
    case PclRESP: return "PclRESP";
    case PclKEEPRESP: return "PclKEEPRESP";
    case PclABORT: return "PclABORT";
    case PclCANCEL: return "PclCANCEL";
    case PclSUCCESS: return "PclSUCCESS";
    case PclFAILURE: return "PclFAILURE";
    case PclERROR: return "PclERROR";
    case PclRECORD: return "PclRECORD";
    case PclENDSTATEMENT: return "PclENDSTATEMENT";
    case PclENDREQUEST: return "PclENDREQUEST";
    case PclFMREQ: return "PclFMREQ";
    case PclFMRUNSTARTUP: return "PclFMRUNSTARTUP";
    case PclVALUE: return "PclVALUE";
    case PclNULLVALUE: return "PclNULLVALUE";
    case PclOK: return "PclOK";
    case PclFIELD: return "PclFIELD";
    case PclNULLFIELD: return "PclNULLFIELD";
    case PclLOGON: return "PclLOGON";
    case PclLOGOFF: return "PclLOGOFF";
    case PclDATAINFO: return "PclDATAINFO";
    case PclOPTIONS: return "PclOPTIONS";
    case PclPREPINFO: return "PclPREPINFO";
    case PclPREPINFOX: return "PclPREPINFOX";
    case PclXDIX: return "PclDATAINFOX";
    default:
                  {
                    static char buf[64];
                    sprintf(buf, "flavor_%d", flavor);
                    return buf;
                  }
  }
}

  void
Init_cli(void)
{
  VALUE Teradata, Teradata_Error;

  Teradata = rb_define_module("Teradata");
  Teradata_Error = rb_const_get(Teradata, rb_intern("Error"));

  CLI = rb_define_class_under(Teradata, "CLI", rb_cObject);
  rb_define_const(CLI, "Id", rb_str_new2("$Id: cli.c 629 2010-02-17 01:46:11Z aamine $"));
  rb_define_singleton_method(CLI, "cleanup", cli_cleanup, 0);
  rb_define_alloc_func(CLI, cli_s_allocate);
  rb_define_private_method(CLI, "initialize", cli_initialize, 2);
  rb_define_method(CLI, "logoff", cli_logoff, 0);
  rb_define_method(CLI, "logging_on?", cli_logging_on_p, 0);
  rb_define_method(CLI, "session_charset", cli_session_charset, 0);
  rb_define_method(CLI, "fetch", cli_fetch, 0);
  rb_define_method(CLI, "send_request", cli_send_request, 1);
  rb_define_method(CLI, "end_request", cli_end_request, 0);
  rb_define_method(CLI, "message", cli_message, 0);
  rb_define_method(CLI, "data", cli_data, 0);
  rb_define_method(CLI, "flavor_name", cli_flavor_name, 0);

  CLIError = rb_define_class_under(Teradata, "CLIError", Teradata_Error);
}
