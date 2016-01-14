// Copyright 2015 Yahoo! Inc.
// Author: Tim Crowder
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <zookeeper.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/errno.h>
#include <regex.h>
#include <time.h>

#include <string>
#include <vector>
#include <map>

#include <readline/readline.h>
#include <readline/history.h>

using namespace std;

class FileCommand;
typedef map<string, FileCommand*>::iterator CommandIterator;

const char* WHITESPACE = " \t\f";
string pwdStr("/");
int done;
string servers;
zhandle_t *zh;
clientid_t myid;
volatile bool connected;


#define ifstr(var, val) if (var==val) { return #val; }
const char* ZooStatusToStr(int zstatus) {
  ifstr(zstatus, ZOK)
  ifstr(zstatus, ZSYSTEMERROR)
  ifstr(zstatus, ZRUNTIMEINCONSISTENCY)
  ifstr(zstatus, ZDATAINCONSISTENCY)
  ifstr(zstatus, ZCONNECTIONLOSS)
  ifstr(zstatus, ZMARSHALLINGERROR)
  ifstr(zstatus, ZUNIMPLEMENTED)
  ifstr(zstatus, ZOPERATIONTIMEOUT)
  ifstr(zstatus, ZBADARGUMENTS)
  ifstr(zstatus, ZINVALIDSTATE)
  ifstr(zstatus, ZAPIERROR)
  ifstr(zstatus, ZNONODE)
  ifstr(zstatus, ZNOAUTH)
  ifstr(zstatus, ZBADVERSION)
  ifstr(zstatus, ZNOCHILDRENFOREPHEMERALS)
  ifstr(zstatus, ZNODEEXISTS)
  ifstr(zstatus, ZNOTEMPTY)
  ifstr(zstatus, ZSESSIONEXPIRED)
  ifstr(zstatus, ZINVALIDCALLBACK)
  ifstr(zstatus, ZINVALIDACL)
  ifstr(zstatus, ZAUTHFAILED)
  ifstr(zstatus, ZCLOSING)
  ifstr(zstatus, ZNOTHING)
  ifstr(zstatus, ZSESSIONMOVED)
  return "<Zoo Status Unknown>";
}



const char* GetZooErrorReason(int rc) {
  if (rc) {
    switch (rc) {
      // system/server-side errors
      case ZBADARGUMENTS            : return "invalid input parameters";
      case ZINVALIDSTATE            : return "bad zookeeper state";
      case ZMARSHALLINGERROR        : return "marshall error (out of mem?)";
      // API errors
      case ZNONODE                  : return "node does not exist";
      case ZNOAUTH                  : return "permission error";
      case ZBADVERSION              : return "node version check failed";
      case ZNOCHILDRENFOREPHEMERALS : return "ephemeral nodes can't have children";
      case ZNODEEXISTS              : return "node already exists";
      case ZNOTEMPTY                : return "directory not empty";
      case ZINVALIDACL              : return "invalid ACL";
      case ZAUTHFAILED              : return "client auth failed";
    }
    return "<unknown>";
  }
  return NULL;
}


#ifndef CERT_UTIL
#define CERT_UTIL "zoosh-cert-util"
#define CERT_ARGS "--show"
#define CERT_TYPE "auth-type"
#endif

int GetCert(const std::string& role, std::string& cert) {
  FILE *fpipe;
  int status;
  char buf[1024];

  std::string cmd = CERT_UTIL " " CERT_ARGS;
  cmd += role;
  fpipe = (FILE*)popen(cmd.c_str(), "r");
  if (!fpipe) {
    fprintf(stderr, "Error on popen for " CERT_UTIL "\n");
    return -1;
  }
  char* read = fgets(buf, sizeof(buf), fpipe);
  if (!read) {
    fprintf(stderr, "Error reading response from " CERT_UTIL "\n");
    return -1;
  }
  status = pclose(fpipe);
  if (status) {
    fprintf(stderr, "Error %d on pclose for " CERT_UTIL "\n", status);
    return -1;
  }
  int len = strlen(buf);
  // remove trailing newline
  if (buf[len-1] == '\n') { buf[len-1]=0; }
  char *sep = strstr(buf, ": ");
  if (!sep) {
    fprintf(stderr, "Error parsing " CERT_UTIL " response [%s]\n", buf);
    return -1;
  }
  cert = (sep+2); // skip ": "
  if (cert == "NOT FOUND") {
    return -1;
  }
  return 0;
}



// Strip whitespace from the start and end of 'text'.
void StripWhite(string &text) {
  string::size_type s = text.find_first_not_of(WHITESPACE);
  string::size_type e = text.find_last_not_of(WHITESPACE);

  if (s==string::npos || e==string::npos) {
    text.empty();
  } else {
    text = text.substr(s, e+1-s);
  }
}

// split a string on any character in delimiters
int SplitString(string &str, vector<string> &tokens, const string& delims) {
  string::size_type s = 0;
  string::size_type e = str.find_first_of(delims, s);

  tokens.clear();
  while ((e!=string::npos)) {
    string tmp = str.substr(s, e-s);
    tokens.push_back(tmp);
    s = str.find_first_not_of(delims, e);
    if (s==string::npos) { break; }
    e = str.find_first_of(delims, s);
  }
  if (s!=string::npos) {
    string tmp = str.substr(s, str.size()+1-s);
    tokens.push_back(tmp);
  }
  return tokens.size();
}

// split a string on whitespaces
int SplitTokens(string &cmd, vector<string> &tokens) {
  return SplitString(cmd, tokens, WHITESPACE);
  //string::size_type s = 0;
  //string::size_type e = cmd.find_first_of(WHITESPACE, s);

  //tokens.clear();
  //while ((e!=string::npos)) {
  //  string tmp = cmd.substr(s, e-s);
  //  tokens.push_back(tmp);
  //  s = cmd.find_first_not_of(WHITESPACE, e);
  //  if (s==string::npos) { break; }
  //  e = cmd.find_first_of(WHITESPACE, s);
  //}
  //if (s!=string::npos) {
  //  string tmp = cmd.substr(s, cmd.size()+1-s);
  //  tokens.push_back(tmp);
  //}
  //return tokens.size();
}



int MergePath(string &dst, const string &sub) {
  string::size_type found;
  string newDir = dst;

  if (sub.size() && '/'==sub[0]) {
    newDir = sub;
  } else {
    // ensure a slash between directories
    if ('/' != newDir[newDir.size()-1]) {
      newDir += '/';
    }
    newDir += sub;
  }
  // ensure trailing slash
  if ('/' != newDir[newDir.size()-1]) {
    newDir += '/';
  }

  // simplify any parent-relative paths
  while (string::npos != (found = newDir.find("/../")) ) {
    string::size_type start = newDir.rfind('/', found-1);
    if (start==string::npos) {
      printf("Can't cd to non existent directory [%s] from [%s]\n", sub.c_str(), dst.c_str());
      return (1);
    }
    newDir.erase(start, (found+3-start));
  }

  // remove any no-op paths
  while (string::npos != (found=newDir.find("/./")) ) {
    newDir.erase(found, 2);
  }
  while (string::npos != (found=newDir.find("//")) ) {
    newDir.erase(found, 1);
  }

  dst = newDir;
  return 0;
}

int FinalizePath(string& path) {
  if (path.size()==0 || path[0]!='/') {
    string tmp = pwdStr;
    MergePath(tmp, path);
    path = tmp;
  }
  if (path.size()<2) {
    return 0;
  }
  if ('/' == path[path.size()-1]) {
    path.resize(path.size()-1);
    return 0;
  }
  return 0;
}

bool GetParentDirectory(const string &path, string &parent) {
  string::size_type sep = path.find_last_of("/");
  if (sep==string::npos || sep==0) {
    return false;
  }
  parent = path.substr(0, sep);
  return true;
}
bool IsWildcard(const string& arg) {
  return 
    (arg.find_first_of("*?[]") != string::npos);
}
bool IsOption(const string& arg) {
  return (arg[0]=='-' || arg[0] == '+');
}


int GetDirectoryListing(const string& path, vector<string> &entries) {
  //printf("listing directory %s\n", path.c_str());
  struct String_vector list;
  int rc = zoo_get_children(zh, path.c_str(), 0, &list);
  if (rc) { return rc; }
  for (int i=0; i<list.count; ++i) {
    entries.push_back(list.data[i]);
  }
  return 0;
}

// assumes the wildcard is at the end of the string
// doesn't do recursive matching
// TODO just split by directory separator '/' and expand into a tree...
//   this doesn't allow for wildcard parts that span dir/subdir, but is much better
int GlobWildcardTail(const string& pattern, vector<string> &matched) {
  string parent;
  regex_t regex;
  regmatch_t match;
  vector<string> entries;

  // force full line match?
  //fullpat = "^"; fullpat += pattern; fullpat += "$";
  if (regcomp(&regex, pattern.c_str(), REG_EXTENDED|REG_NOSUB)) {
    printf("REGEX compiling %s FAILED \n", pattern.c_str());
    return -1;
  }
  GetParentDirectory(pattern, parent);
  GetDirectoryListing(parent, entries);
  for (int i=entries.size()-1; i>=0; --i) {
    string current = parent; current += '/'; current += entries[i];
    if (0==regexec(&regex, current.c_str(), 1, &match, 0)) {
      matched.push_back(current);
    }
  }
  regfree(&regex);
  return 0;
}

// tries to read the contents of a file into 'data'
// returns node-size on success (may be > 'maxlen')
// returns <0 on failure
int ReadFile(const string& path, string& data, int maxlen=-1) {
    int rc, len, buflen;
    struct stat st;
    string src = path;
    //FinalizePath(src);

    rc = stat(src.c_str(), &st);
    if (rc) {
      printf("Error stat (local-fs) on [%s] %d: %s\n", src.c_str(), errno, strerror(errno));
      return -1;
    }

    int fd = open(src.c_str(), O_RDONLY);
    if (fd<0) {
      printf("Error open (local-fs) on [%s] %d: %s\n", src.c_str(), errno, strerror(errno));
      return -1;
    }

    buflen = st.st_size;
    len = buflen;
    if ((maxlen > 0) && (maxlen < len)) {
      len = maxlen;
    }
    char buff[len];
    ssize_t bytes = read(fd, buff, len);
    close(fd);
    if (bytes<0) {
      printf("Read failed (local-fs) on [%s], %s\n", src.c_str(), strerror(errno));
      return -1;
    }
    data.assign(buff, bytes);
    return buflen;
}


// tries to read the contents of a ZK node into 'data'
// returns node-size on success (may be > 'maxlen')
// returns <0 on failure
int ReadNode(const string& path, string& data, int maxlen=-1) {
    int rc, len, buflen;
    struct Stat st;
    string src = path;
    FinalizePath(src);

    rc=zoo_exists(zh, src.c_str(), 0, &st);
    if (rc) {
      printf("Error stat on [%s] %d: %s\n", src.c_str(), rc, zerror(rc));
      return -1;
    }

    // NOTE: current jute settings limit node size to < 1MB.
    buflen = st.dataLength;
    len = buflen;
    if ((maxlen > 0) && (maxlen < len)) {
      len = maxlen;
    }

    char buff[len];
    rc = zoo_get(zh, src.c_str(), 0, buff, &len, NULL);
    if (rc) {
      const char* reason = GetZooErrorReason(rc);
      printf("Read (get) failed on [%s], %s\n", src.c_str(), reason);
      return -1;
    }
    data.assign(buff, len);
    return buflen;
}

int WriteFile(const string& path, string& data) {
    string dest = path;
    //FinalizePath(dest);
    int fd = open(dest.c_str(), O_WRONLY|O_CREAT, 0644);
    if (fd<0) {
      printf("Error open (local-fs, write) on [%s] %d: %s\n", dest.c_str(), errno, strerror(errno));
      return -1;
    }
    ssize_t bytes = write(fd, data.data(), data.size());
    close(fd);
    if (bytes<0) {
      printf("Read failed (local-fs) on [%s], %s\n", dest.c_str(), strerror(errno));
      return -1;
    }
    return bytes;
}

int WriteNode(const string& path, string& data) {
    string dest = path;
    FinalizePath(dest);
    int flags = 0;
    int rc = zoo_create(zh, dest.c_str(), data.data(), data.size(), &ZOO_OPEN_ACL_UNSAFE, flags, NULL, 0);
    if (rc == ZNODEEXISTS) {
        rc = zoo_set(zh, dest.c_str(), data.data(), data.size(), -1);
    }
    if (rc) {
        fprintf(stderr, "ERROR creating node [%s] for %d -- %s\n", dest.c_str(), rc, zerror(rc));
        return rc;
    }
    return data.size();
}


////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

class FileCommand {
public:
  static map<string, FileCommand*> commands;
public:
  string name;
  string desc;
public:
  FileCommand() : name("<unknown>"), desc("<unknown>") {}
  FileCommand(const char* name_, const char* desc_) : name(name_), desc(desc_) {}
  virtual ~FileCommand() {}

  static int RegisterCommand(FileCommand* cmd) {
    commands[cmd->name] = cmd;
    return commands.size();
  }
  // return a FileCommand or NULL if not found
  static FileCommand* FindCommand(const char* name) {
    CommandIterator found = commands.find(name);
    if (found != commands.end()) {
      return found->second;
    }
    return ((FileCommand *)NULL);
  }

  virtual bool DoFullPaths() { return true; } 
  virtual bool DoGlobPaths() { return true; } 

  virtual bool WarnNoGlobMatch() { return false; }
  virtual bool ErrorNoGlobMatch() { return false; }

  virtual int ExpandArgs(const vector<string> &argsIn, vector<string> &argsOut) {
    if (!DoFullPaths() && !DoGlobPaths()) {
      argsOut = argsIn;
      return 0;
    }
    // push the command name
    argsOut.push_back(argsIn[0]);
    for (unsigned i=1; i<argsIn.size(); ++i) {
      const string &word=argsIn[i];
      if (IsOption(word)) {
        argsOut.push_back(word);
      } else if ((word.size()>0) && (word.data()[0]==':')) {
        // don't mess with local paths yet
        // TODO add `pwd`
        argsOut.push_back(word);
      } else if (DoGlobPaths() && IsWildcard(word)) {
        vector<string> entries;
        GlobWildcardTail(word, entries);
        if (entries.size() == 0) {
          if (ErrorNoGlobMatch()) {
            fprintf(stderr, "ERROR: %s No Match %s\n", argsIn[0].c_str(), word.c_str());
            return -1;
          } else if (WarnNoGlobMatch()) {
            printf("Warn: %s No Match %s\n", argsIn[0].c_str(), word.c_str());
          }
        }
        for (unsigned c=0; c<entries.size(); ++c) {
          if (DoFullPaths()) {
            string path(pwdStr);
            MergePath(path, entries[i]);
            // strip trailing dir-slash
            if (path.size()>1) {
              path=path.substr(0, path.size()-1);
            }
            argsOut.push_back(path);
          } else {
            argsOut.push_back(entries[i]);
          }
        }
      } else {
        string path(pwdStr);
        MergePath(path, word);
        // strip trailing dir-slash
        if (path.size()>1) {
          path=path.substr(0, path.size()-1);
        }
        argsOut.push_back(path);
      }
    }

    return 0;
  }

  // args[0] is the command itself
  virtual int Run(const vector<string> &args) {
    if (args.size()==1) {
      // call the function with no args
      return (Run());
    } else {
      unsigned i;
      // loop over args, so commands don't have to deal with args
      // TODO do absolute path conversion here...
      for (i=1; i<args.size(); ++i) {
        const string &word=args[i];
        if (IsWildcard(word)) {
          vector<string> entries;
          // put the command itself in...
          entries.push_back(args[0]);
          GlobWildcardTail(word, entries);
          if (entries.size()>1) {
            int rc = Run(entries);
            if (rc) { return rc; }
          } else {
            printf("%s No Matches: %s\n", args[0].c_str(), word.c_str());
          }
        } else {
          // Call the function.
          int rc = (Run(word));
          if (rc) { return rc; }
        }
      }
    }
    return (0);
  }
  virtual int Run() { 
    printf("command %s can't run without a filename\n", name.c_str());
    return -1;
  }
  virtual int Run(const string& arg) {
    printf("command %s can't run with a filename\n", desc.c_str());
    return -1;
  }
};

map<string, FileCommand*> FileCommand::commands;



class ListState {
public:
  bool recurse; // recursive listing
  bool full;    // show full path
  bool zstat;   // show ctime/mtime/version/dataLength/ephem
  bool sec;     // show perms/ACLs

public:
  ListState() { Reset(); }
  void Reset() {
    recurse=false; 
    full=false; 
    zstat=false; 
    sec=false;
  }
  int ParseArg(const string& arg) {
    for (unsigned j=1; j<arg.size(); ++j) {
      switch (arg[j]) {
        case 'r' : recurse = true; break;
        case 'f' : full = true;    break;
        case 'l' : zstat = true;   break;
        case 'z' : sec = true;     break;
        default: 
          fprintf(stderr, "invalid option [%s]\n", arg.c_str());
          return -1;
      }
    }
    return 0;
  }
};

//struct Stat {
//    int64_t czxid; int64_t mzxid; int64_t pzxid;
//    int64_t ctime; int64_t mtime;
//    int32_t version; int32_t cversion; int32_t aversion;
//    int64_t ephemeralOwner;
//    int32_t dataLength;
//    int32_t numChildren;
//};

//struct Id {
//    char * scheme;
//    char * id;
//};
//struct ACL {
//    int32_t perms;
//    struct Id id;
//};
//struct ACL_vector {
//    int32_t count;
//    struct ACL *data;
//};

int GetStatAclString(const string& path, string* stats, string* acls) {
  struct Stat zks;
  struct ACL_vector aclVec;
  char tbuf[128];
  char buf[1024];
  int i;

  int rc = zoo_get_acl(zh, path.c_str(), &aclVec, &zks);
  if (rc) {
    return rc;
  }
  if (acls) {
    //*acls += "ACLs: \n";
    for (i=0; i<aclVec.count; ++i) {
      *acls += "  ";
      ACL *acl = &(aclVec.data[i]);
      // loop over acls in aclVec
      *acls += (acl->perms & ZOO_PERM_CREATE) ? "c":"-"; 
      *acls += (acl->perms & ZOO_PERM_DELETE) ? "d":"-"; 
      *acls += (acl->perms & ZOO_PERM_READ)   ? "r":"-";
      *acls += (acl->perms & ZOO_PERM_WRITE)  ? "w":"-";
      *acls += (acl->perms & ZOO_PERM_ADMIN)  ? "a":"-";
      *acls += "  ";
      *acls += acl->id.scheme; *acls += "::"; *acls += acl->id.id; *acls += "  \n";
    }
    //printf("%s", acls->c_str());
  }

  if (stats) {
    time_t t = (time_t)(zks.mtime/1000);
    struct tm *tmp;
    tmp = localtime(&t);
    strftime(tbuf, sizeof(tbuf), "%Y/%m/%d-%H:%M:%S ", tmp);
    
    // version and aversion seem to be uniformly 0
    snprintf(buf, 1023, "%4.4ld %s v:%3.3ld cv:%3.3ld eph:%ld", 
        (long)zks.dataLength, tbuf,
        (long)zks.version,
        (long)zks.cversion,
        (long)(zks.ephemeralOwner ? 1 : 0)
    );
    buf[1023]='0';
    *stats = buf;
  }

  return 0;
}

int ListFile(const string& pref, const string& path0, const ListState& state) {
  string path = path0;
  FinalizePath(path);
  struct String_vector list;

  //printf("[%s]\n", path.c_str());
  int rc = zoo_get_children(zh, path.c_str(), 0, &list);
  if (rc==0) {
    for (int i=0; i<list.count; ++i) {
      string sub = path;
      MergePath(sub, list.data[i]);
      FinalizePath(sub);
      string subPref = pref;
      string stats;
      string acls;

      if (state.zstat || state.sec) {
        GetStatAclString(sub, state.zstat?&stats:NULL, state.sec?&acls:NULL);
      }
      if (state.zstat) {
        printf("%s ", stats.c_str());
      }
      if (state.full) {
        // sub has a trailing '/' ...
        //printf("%s%s%s%s\n", stats.c_str(), pref.c_str(), path.c_str(), list.data[i]);
        printf("%s%s\n", pref.c_str(), sub.c_str());
      } else {
        printf("%s%s\n", pref.c_str(), list.data[i]);
        subPref += "  ";
      }
      if (state.sec) {
        printf("%s", acls.c_str());
      }

      if (state.recurse) {
        ListFile(subPref, sub, state);
      }
    }
    deallocate_String_vector(&list);
  }
                            
  return rc;
}

class ListCommand : public FileCommand {
public:
  ListCommand() : FileCommand("ls", "List nodes under the current node.\n"
          "  -r recursive\n  -f full path\n  -l long listing\n  -z print ACLs") {}
  virtual int Run() {
    string dummy;
    return Run(dummy);
  }
  virtual int Run(const vector<string> &args) {
    state.Reset();
    vector<string> fileArgs;
    for (unsigned i=0; i<args.size(); ++i) {
      const string& cur = args[i];
      if (cur.size() && cur[0]=='-') {
       if (state.ParseArg(cur)) {
          return -1;
       }
      } else {
        fileArgs.push_back(cur);
      }
    }
    return FileCommand::Run(fileArgs);
  }
  virtual int Run(const string& arg0) {
    return ListFile("  ", arg0, state);
  }
public:
  ListState state;
};
int ListDummy = FileCommand::RegisterCommand(new ListCommand);

class CatCommand : public FileCommand {
public:
  CatCommand() : FileCommand("cat", "Show node contents.") {}
  virtual int Run(const string &arg0) {
    string data;
    int maxlen = 65536;
    int len = ReadNode(arg0, data, maxlen);
    printf("[%s] %d bytes: \n", arg0.c_str(), len);
    if (len<0) {
      printf("<error>\n");
    } else if (len==0) {
      printf("<empty>\n");
    } else {
      bool truncated = false;
      if (len>maxlen) { 
        len = maxlen; 
        truncated = true;
      }
      printf("%s\n", data.c_str());
      if (truncated) {
        printf("<truncated>\n");
      }
    }
    // TODO cat arbitrary sized files
    //   NOTE: current jute settings limit node size to < 1MB.
    return (0);
  }
};
int CatDummy = FileCommand::RegisterCommand(new CatCommand);


int ParseACL(const std::string& aclStr, struct ACL* acl) {
  unsigned i;
  bool done = false;
  int32_t perms = 0;
  std::string scheme;
  std::string id;
  //fprintf(stderr, "NOTE parsing acl [%s]\n", aclStr.c_str());

  for (i=0; (!done) && i<aclStr.size(); ++i) {
    // loop over acls in aclVec
    switch (aclStr[i]) {
      case 'c' : perms |= ZOO_PERM_CREATE; break;
      case 'd' : perms |= ZOO_PERM_DELETE; break;
      case 'r' : perms |= ZOO_PERM_READ;   break;
      case 'w' : perms |= ZOO_PERM_WRITE;  break;
      case 'a' : perms |= ZOO_PERM_ADMIN;  break;
      default : 
        done=true; break;
    }
  }
  if (i==aclStr.size()) {
    acl->perms = perms;
    acl->id = ZOO_AUTH_IDS;
    return 0;
  }
  if (i>=aclStr.size() || !i || aclStr[i-1]!=':') {
    // error
    fprintf(stderr, "Error parsing acl [%s]\n", aclStr.c_str());
    return -1;
  }
  // parse out scheme and id
  size_t idSep = aclStr.find("::", i+1);
  if (idSep == string::npos) {
    fprintf(stderr, "Error parsing acl [%s]\n", aclStr.c_str());
    return -1;
  }
  scheme = aclStr.substr(i, idSep-i);
  id = aclStr.substr(idSep+2);
  
  acl->perms = perms;
  acl->id.id = strdup(id.c_str());
  acl->id.scheme = strdup(scheme.c_str());
  return 0;
}


class ChmodCommand : public FileCommand {
public:
  ACL_vector aclVec;
public:
  ChmodCommand() : FileCommand("chmod", "change node permissions.") {}
  virtual int Run(const vector<string> &args) {
    int rc = 0;
    if (args.size()<2) {
      fprintf(stderr, "Error chmod missing acl or filename\n");
      return -1;
    }
    // create aclVec  
    allocate_ACL_vector(&aclVec, 1);
    // parse first arg as "[cdrwa]:scheme::id"
    if (ParseACL(args[1], &aclVec.data[0])) {
      // error already printed
      // fall thru to cleanup
    } else {
      vector<string> fileArgs;
      fileArgs.push_back(args[0]);
      for (unsigned i=2; i<args.size(); ++i) {
        const string& cur = args[i];
        if (cur.size() && cur[0]=='-') {
         // TODO parse additional args or complain
         //if (state.ParseArg(cur)) {
         //   return -1;
         //}
        } else {
          fileArgs.push_back(cur);
        }
      }
      rc = FileCommand::Run(fileArgs);
    }
    // free acl       
    if (ZOO_AUTH_IDS.id == aclVec.data[0].id.id &&
        ZOO_AUTH_IDS.scheme == aclVec.data[0].id.scheme) {
      aclVec.data[0].id.id=NULL;
      aclVec.data[0].id.scheme=NULL;
    }
    deallocate_ACL_vector(&aclVec);
    return rc;
  }
  virtual int Run(const string& arg0) {
    int rc;
    string arg = arg0;
    FinalizePath(arg);

    //struct ACL acl = aclVec.data[0];
    //fprintf(stdout, "node:[%s] perm:0x%x scheme:[%s] id:[%s]\n", arg.c_str(), acl.perms, acl.id.scheme, acl.id.id);
    rc = zoo_set_acl(zh, arg.c_str(), -1, &aclVec);

    if (rc) {
      printf("Error chmod on [%s] %d: %s\n", arg.c_str(), rc, zerror(rc));
      return rc;
    }
    return (0);
  }
};
int ChmodDummy = FileCommand::RegisterCommand(new ChmodCommand);


class CopyCommand : public FileCommand {
public:
  CopyCommand() : FileCommand("cp", "Copy a file/node.\nNOTES:\n  zk source and dest should include the filename.\n  Prefix local (non-ZK) files with ':' .") {}

  virtual int Run(const string &arg0, const string &arg1) {
    string src = arg0;
    string dest = arg1;

    int rc = 0;
    string data;
    if (src.c_str()[0] == ':') {
      rc = ReadFile(src.substr(1), data);
    } else {
      rc = ReadNode(src, data);
    }
    if (rc<0) {
      printf("Copy failed (read) [%s to %s]\n", src.c_str(), dest.c_str());
      return -1;
    }
    if (dest.c_str()[0] == ':') {
      rc = WriteFile(dest.substr(1), data);
    } else {
      rc = WriteNode(dest, data);
    }
    if (rc<0) {
      printf("Copy failed (write) [%s to %s]\n", src.c_str(), dest.c_str());
      return -1;
    }
    return 0;
  }

  virtual int Run(const vector<string> &argsIn) {
    vector<string> args;
    int rc = ExpandArgs(argsIn, args);
    if (rc) { return rc; }
    vector<string> options;
    vector<string> fileArgs;
    // skip command name...
    for (unsigned i=1; i<args.size(); ++i) {
      const string& cur = args[i];
      if (IsOption(cur)) {
       //if (state.ParseArg(cur)) {
          options.push_back(cur);
          printf("Command %s: unexpected option [%s]!\n", args[0].c_str(), cur.c_str());
          return -1;
       //}
      } else {
        fileArgs.push_back(cur);
      }
    }
    if (fileArgs.size() != 2) {
      printf("Command %s: expects exactly 2 full filenames, got %d!\n", args[0].c_str(), (int)fileArgs.size());
    }
    return Run(fileArgs[0], fileArgs[1]);
  }

};
int CopyDummy = FileCommand::RegisterCommand(new CopyCommand);



class StatCommand : public FileCommand {
public:
  StatCommand() : FileCommand("stat", "Show node statistics.") {}
  virtual int Run(const string& arg0) {
    string arg = arg0;
    FinalizePath(arg);
    int rc;
    struct Stat st;
    char tctimes[40], tmtimes[40];
    time_t tctime, tmtime;

    if ( (rc=zoo_exists(zh, arg.c_str(), 0, &st)) ) {
      printf("Error stat on [%s] %d: %s\n", arg.c_str(), rc, zerror(rc));
      return (1);
    }

    printf("[%s]:\n", arg.c_str());

    tctime = st.ctime/1000;
    tmtime = st.mtime/1000;
    fprintf(stderr, "\tctime = %s\tczxid=%llx\n" "\tmtime=%s\tmzxid=%llx\n"
    "\tversion=%x\taversion=%x\n" "\tephemeralOwner = %llx\n",
    ctime_r(&tctime, tctimes), (long long unsigned)st.czxid, 
    ctime_r(&tmtime, tmtimes), (long long unsigned)st.mzxid,
    (unsigned int)st.version, (unsigned int)st.aversion,
    (long long unsigned)st.ephemeralOwner);

    return (0);
  }
};
int StatDummy = FileCommand::RegisterCommand(new StatCommand);



class TouchCommand : public FileCommand {
public:
  int flags;

public:
  TouchCommand() : FileCommand("touch", "Touch (create) node(s).\n  -e for ephemeral\n  -s for sequential."), flags(0) {}
  virtual int Run() {
    string dummy;
    return Run(dummy);
  }
  virtual int Run(const vector<string> &argsIn) {
    flags = 0;
    return FileCommand::Run(argsIn);
  }

  virtual int Run(const string& arg0) {
    if (IsOption(arg0)) {
      if (arg0 == "-e") {
        flags |= ZOO_EPHEMERAL;
      } else if (arg0 == "-s") {
        flags |= ZOO_SEQUENCE;
      } else {
        printf("Unknown option [%s]!\n", arg0.c_str());
        return -1;
      }
      return 0;
    }

    string arg = arg0;
    FinalizePath(arg);
    // TODO deal with ephemeral and sequence flags
    char createdPath[1024];
    string data;
    //bool ephemeral = false;
    //bool sequence = false;
    //int flags = (ephemeral ? ZOO_EPHEMERAL : 0) | (sequence ? ZOO_SEQUENCE : 0 );
    int rc = zoo_create(zh, arg.c_str(), data.data(), data.size(), &ZOO_OPEN_ACL_UNSAFE, flags, createdPath, 1023);
    if (rc) {
        fprintf(stderr, "ERROR creating node [%s] for %d -- %s\n", arg.c_str(), rc, zerror(rc));
        return rc;
    }
    if (arg!=createdPath) {
      printf("Created %s\n", createdPath);
    }
                              
    return rc;
  }
};
int TouchDummy = FileCommand::RegisterCommand(new TouchCommand);

class DelCommand : public FileCommand {
public:
  bool recurse;
public:
  DelCommand() : FileCommand("rm", "Delete node(s).") {}
  virtual int Run(const vector<string> &args) {
    int rc = 0;
    recurse = false;
    if (args.size()<2) {
      fprintf(stderr, "Error: Missing file to remove.\n");
      return -1;
    }
    vector<string> fileArgs;
    fileArgs.push_back(args[0]);
    for (unsigned i=1; i<args.size(); ++i) {
      const string& cur = args[i];
      if (cur=="-r") {
        recurse = true;
      } else {
        fileArgs.push_back(cur);
      }
    }
    rc = FileCommand::Run(fileArgs);
    return rc;
  }
  virtual int Run(const string& arg0) {
    string arg = arg0;
    FinalizePath(arg);
    if (recurse) {
      struct String_vector list;
      int rc = zoo_get_children(zh, arg.c_str(), 0, &list);
      if (rc==0) {
        for (int i=0; i<list.count; ++i) {
          string sub = arg;
          MergePath(sub, list.data[i]);
          FinalizePath(sub);
          rc = Run(sub);
          if (rc) { 
            deallocate_String_vector(&list);
            return rc;
          }
        }
        deallocate_String_vector(&list);
      } else {
        printf("Delete failed (listing children) on [%s], %s\n", arg.c_str(), ZooStatusToStr(rc));
        return (1);
      }
    }
    // pass version of -1 to avoid the version check
    int rc = zoo_delete(zh, arg.c_str(), -1);
    if (rc) {
      const char* reason = GetZooErrorReason(rc);
      printf("Delete failed on [%s], %s\n", arg.c_str(), reason);
      return (1);
    }
    return (0);
  }
};
int DelDummy = FileCommand::RegisterCommand(new DelCommand);

class HelpCommand : public FileCommand {
public:
  HelpCommand() : FileCommand("?", "Show Help.") {}
  HelpCommand(const char* name, const char* desc) : FileCommand(name, desc) {}

  virtual int GetMaxCmdWidth() {
    CommandIterator iter;
    int maxLen = 1;
    for (iter=commands.begin(); iter!=commands.end(); ++iter) {
      int len = iter->first.size();
      if (len>maxLen) { maxLen = len; }
    }
    return maxLen;
  }

  virtual void PrintCmdHelp(CommandIterator iter) {
    int maxLen = GetMaxCmdWidth();
    vector<string> lines;
    SplitString(iter->second->desc, lines, "\n");
    printf("%-*s%s.\n", maxLen+3, iter->first.c_str(), lines[0].c_str());
    for (unsigned l=1; l<lines.size(); ++l) {
      printf("%-*s%s.\n", 3, " ", lines[l].c_str());
    }
  }

  virtual int Run() {
    CommandIterator iter;
    for (iter=commands.begin(); iter!=commands.end(); ++iter) {
      PrintCmdHelp(iter);
    }
    return (0);
  }
  virtual int Run(const string& arg) {
    int printed = 0;

    CommandIterator iter;
    for (iter=commands.begin(); iter!=commands.end(); ++iter) {
      if (iter->first == arg) {
        PrintCmdHelp(iter);
        printed++;
      }
    }

    if (!printed) {
      printf("No commands match '%s'\n", arg.c_str());
      Run();
    }
    return (0);
  }
};
int HelpDummy = FileCommand::RegisterCommand(new HelpCommand);
int HelpDummy2 = FileCommand::RegisterCommand(new HelpCommand("help", "Show Help."));

class CdCommand : public FileCommand {
public:
  CdCommand() : FileCommand("cd", "Change working node (directory).") {}
  virtual int Run(const string& arg0) {
    string arg = arg0;
    FinalizePath(arg);

    pwdStr=arg;
    printf("[%s]\n", pwdStr.c_str());
    return (0);
  }
};
int CdDummy = FileCommand::RegisterCommand(new CdCommand);

class PwdCommand : public FileCommand {
public:
  PwdCommand() : FileCommand("pwd", "Print working node (directory).") {}
  virtual int Run() {
    printf("[%s]\n", pwdStr.c_str());
    return 0;
  }
};
int PwdDummy = FileCommand::RegisterCommand(new PwdCommand);

class ExitCommand : public FileCommand {
public:
  ExitCommand(const char* name, const char* help) : FileCommand(name, help) {}
  virtual int Run() {
    done = 1;
    return (0);
  }
};
int ExitDummy = FileCommand::RegisterCommand(new ExitCommand("exit", "Exit the program."));
int QuitDummy = FileCommand::RegisterCommand(new ExitCommand("quit", "Exit the program."));



///////////////////////////////////////////////////////////////////
// 
///////////////////////////////////////////////////////////////////

// Execute a command line.
int execute_line(string &line) {
  vector<string> tokens;
  FileCommand *command;
  const char *cmd;

  SplitTokens(line, tokens);
  if (!tokens.size()) {
    return (1);
  }

  cmd = tokens[0].c_str();
  command = FileCommand::FindCommand(cmd);

  if (!command) {
      fprintf(stderr, "%s: No such command.\n", cmd);
      return (-1);
  }

  return command->Run(tokens);
}


///////////////////////////////////////////////////////////////////
//                  Readline Completion Section
///////////////////////////////////////////////////////////////////

// command completion.
char* command_generator(const char* text, int state) {
  static CommandIterator iter = FileCommand::commands.end();
  static int len;

  if (!state) { // first call for this completion, initialize state
    iter = FileCommand::commands.begin();
    len = strlen(text);
  }
  // Return next partial match
  while (iter!=FileCommand::commands.end()) {
    const string &name = iter->first;
    ++iter;
    if (strncmp(name.c_str(), text, len) == 0) { return (strdup(name.c_str())); }
  }
  // If no names matched, then return NULL.
  return (NULL);
}

// Returns a malloc-allocated string representing the longest
// common leading characters of the strings in list.
// If there are none in common, it still returns an allocated
// 1-byte string "", containing the zero character.
char* longestCommonPrefix(char* list[]) {
  int curLen, len = 0;
  char** cur;
  char* str = NULL;
  const char* prefix = "";

  if (list && list[0]) {
    prefix = list[0];
    len = strlen(prefix);
    for (cur=list+1; cur && cur[0]; ++cur) {
      curLen = strlen(*cur);
      if (curLen>len) { curLen=len; }
      for (len=0; len<curLen && (prefix[len]==(*cur)[len]); ++len) { }
      if (!len) { break; }
    }
  }
  str = strndup(prefix, len);
  return str;
}

// Attempt to complete 'text', between 'start' and 'end' in rl_line_buffer.
// Return the array of matches, or NULL if there aren't any.
char** zoo_completion(const char* text, int start, int end) {
  char **matches = (char **)NULL;

  // completions on the local filesystem are not useful...
  rl_filename_completion_desired = 0;

  if (start == 0) {
    // at start of the line, do command completion
    matches = rl_completion_matches(text, command_generator);
  } else {
    //filename completion
    string sub(text);
    string full, part;
    string path=pwdStr;

    string::size_type found = sub.rfind('/');
    if (string::npos != found) {
      full = sub.substr(0, found);
      part = sub.substr(found+1);
    } else {
      part = sub;
    }

    if (MergePath(path, full)) {
      //printf("Merge failed, returning NULL\n");
      return (NULL);
    }
    int plen = path.size();
    if (plen>1 && '/'==path[plen-1]) {
      path.erase(plen-1);
    }

    int j=0;
    struct String_vector list;
    int rc = zoo_get_children(zh, path.c_str(), 0, &list);
    if (rc==0 && list.count) {
      matches = (char**)malloc((list.count+2) * sizeof(char*));
      matches[0] = NULL;
      ++j;
      for (int i=0; i<list.count; ++i) {
        if (!strncmp(part.c_str(), list.data[i], part.size())) {
          string filename(path);
          string cur(list.data[i]);
          MergePath(filename, cur);
          matches[j++] = strdup(filename.c_str());
        }
      }
      if (j>1) {
        matches[j] = NULL;
        matches[0] = longestCommonPrefix(matches+1);
      } else {
        free(matches[0]);
        free(matches);
        matches=NULL;
      }
    }
  }
  if (!matches) {
  } else {
    rl_completion_suppress_append = 1;
  }
  return (matches);
}

void initialize_readline() {
  // Allow conditional parsing of the ~/.inputrc file. 
  rl_readline_name = "Zookeeper";
  // Tell the completer that we want a crack first.
  rl_attempted_completion_function = zoo_completion;
}

void LogCallbackFunction(const char *message) {
  static time_t lastConRefused = 0;
  // these get spammed horribly (sometimes thousands per sec)
  // so limit to 1 per sec.
  if (strstr(message, "Connection refused")) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    time_t now = tv.tv_sec;
    if (now == lastConRefused) { return; }
    lastConRefused = now;
  }
  fprintf(stderr, "%s\n", message);
}
int ZooConnect();

void ZooWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
  //fprintf(stderr, "WatchHelper called on path[%s] type:%d state:%d obj:%p\n", path, type, state, watcherCtx);
  if (type!=ZOO_SESSION_EVENT) {
    fprintf(stderr, "Watch called on path [%s]\n", path);
  } else {
    if (state==ZOO_CONNECTED_STATE) {
      fprintf(stderr, "INFO  zookeeper connected\n");
      connected=true;
      const clientid_t *id = zoo_client_id(zh);
      if (myid.client_id == 0 || myid.client_id != id->client_id) {
        myid = *id;
        fprintf(stderr, "Got a new id: %llx\n", (long long) myid.client_id);
      }
      //Reconnect();
    } else if (state!=ZOO_AUTH_FAILED_STATE) {
      connected=false;
      fprintf(stderr, "WARN  zookeeper disconnected\n");
      if (is_unrecoverable(zh)) {
        fprintf(stderr, "WARN  zookeeper handle is un-recoverable.... re-connecting\n");
        ZooConnect();
      }
      // TODO deal with re-connect
      //  or long term disconnect
    }
  }
}


void ZooClose() {
  if (zh) {
    fprintf(stderr, "Closing zookeeper handle (%p)\n", zh);
    zookeeper_close(zh);
    fprintf(stderr, "Finished closing zookeeper handle (%p)\n", zh);
    zh=NULL;
  }
}

int ZooConnect() {
  printf("Using servers [%s]\n", servers.c_str());
  zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
  //zoo_set_log_stream(dev_null);
  //zoo_deterministic_conn_order(1); // enable deterministic order

  ZooClose();
  zh = zookeeper_init2(servers.c_str(), ZooWatcher, 10000, &myid, NULL, 0, LogCallbackFunction);
  //zh = zookeeper_init(servers.c_str(), ZooWatcher, 10000, &myid, NULL, 0);
  if (!zh) {
    fprintf(stderr, "ERROR %d initializing zookeeper\n", errno);
    return errno;
  }
  return 0;
}

// servers is a comma seperated list of 'ip:port'
int ZooInit(const char *servers_) {
  servers=servers_;
  int rc = ZooConnect();
  if (rc) {
    return rc;
  }
  fprintf(stderr, "Initialized zookeeper handle (%p)\n", zh);
  return 0;
} 


int main(int argc, char** argv) {
  const char* servers = "localhost:2181";
  std::string cert;
  std::string scheme;
  std::string auth;

  int i;
  for (i=1; i<argc; ++i) {
    if (0 == strcmp(argv[i], "-c")) {
      ++i;
      if (argc <= i) {
        fprintf(stderr, "Error: Must provided a role with -c (cert) option!\n");
        exit(1);
      }
      int rc = GetCert(argv[i], cert);
      if (rc) {
        fprintf(stderr, "Error: Failed fetching cert for role [%s]!\n", argv[i]);
        exit(1);
      }
    } else if (0 == strcmp(argv[i], "-a")) {
      ++i;
      if (argc <= i) {
        fprintf(stderr, "Error: Must provided 'scheme:identity' string with -a (auth) option!\n");
        exit(1);
      }
      char* colon = strchr(argv[i], ':');
      if (!colon) {
        fprintf(stderr, "Error: Failed parsing auth for [%s] (must be 'scheme:auth') !\n", argv[i]);
        exit(1);
      }
      scheme.assign(argv[i], colon-argv[i]);
      auth.assign(colon+1);
    } else if (!strcmp(argv[i], "-h")) {
      // TODO print help
    } else {
      servers = argv[i];
    }
  } 

  initialize_readline(); // Bind our completer. 

  ZooInit(servers);
  int tries;
  for (tries=200; tries>0 && !connected; --tries) {
    usleep(10000);
  }
  if (!connected) {
    fprintf(stderr, "Couldn't connect to ZK server(s) [%s]\n", servers);
    exit(-1);
  }
  if (cert.size()) {
    //fprintf(stdout, "Authenticating with cert [[%s]]\n", cert.c_str());
    int zstatus = zoo_add_auth(zh, CERT_TYPE, cert.c_str(), cert.size(), NULL, NULL);
    if(zstatus != ZOK) {
      fprintf(stderr, "Auth Error, status %d(%s) initializing zookeeper, role[%s]\n", zstatus, ZooStatusToStr(zstatus), cert.c_str());
    }
  }
  if (auth.size()) {
    fprintf(stdout, "Authenticating with scheme [[%s]] auth [[%s]]\n", scheme.c_str(), auth.c_str());
    int zstatus = zoo_add_auth(zh, scheme.c_str(), auth.c_str(), auth.size(), NULL, NULL);
    if(zstatus != ZOK) {
      fprintf(stderr, "Auth Error, status %d(%s) initializing zookeeper, role[%s]\n", zstatus, ZooStatusToStr(zstatus), cert.c_str());
    }
  }

  // Loop reading and executing lines until the user quits.
  for ( ; done == 0; ) {
    //  //re-enable auto-complete, in case the completion function had to disable it
    //  rl_bind_key('\t',rl_complete);

    char* tmp = readline(">>  ");
    if (!tmp) break;
    string line = tmp;
    free(tmp);
    // Remove leading and trailing whitespace.
    // if there is anything left, add it to the history list and execute it.
    StripWhite(line);
    if (line.size()) {
      add_history(line.c_str());
      execute_line(line);
    }
  }

  ZooClose();
  return(0);
}

