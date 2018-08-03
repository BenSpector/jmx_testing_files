//Usage:
//node jmx_poller [-port=####] [-sleep=milliseconds]\
//  [-keywords=[!]word1,[&!]word2...]\
//  <machine ip address 1>\
//  <machine ip address 2>\
//  <machine ip address ...>\
//
//
//  -port or -p: sets the designated JMX port to connect to; default 3000
//  -keywords or -kw: comma-separated list of substrings used to determine which kubernetes pods to
//    attempt to connect to. If "word1" is a value in the the list, it matches pod label names that
//    contain the substring "word1". If "!word2" is a value in the list, it matches pod label names
//    that DO NOT contain the substring "word2". A comma in the list, by default, represents an OR
//    in the matching logic; to override this, start the following element with '&' ("&word3", "&!word4").
//    By default this value is "drill,!falkonry".
//  -sleep or -s: sets sleep timeout between scans, milliseconds; default 10000
//  -machine ip address: eg, 127.0.0.1, 129.146.66.103

var jmx = require("jmx"),
  checkError = require("jmx/lib/helpers/error").checkError;
var async = require("async");

const Client = require('kubernetes-client').Client;
const config = require('kubernetes-client').config;

var clients = [];
var parameters = {};

var client_names = [];
var client_pods = [];
var blacklist = {};
var blacklist_candidates = [];

function listAttributes(client, mbean, callback) {
  var self = client.javaJmx;
  self.mbeanServerConnection.queryMBeans(null, mbean, function(instance) {
    instance.getObjectName(function(err, objectName) {
      if (checkError(err, self)) return;
      self.mbeanServerConnection.javaReflection.invokeMethod(
          client.javaJmx.mbeanServerConnection.mbeanServerConnection,
          "getMBeanInfo",
          [ "javax.management.ObjectName" ], // methodParamsClass
          [ objectName ], // methodParams
          callback
        );
    });
  });
}
/*
function listAttributes(client, mbean, callback) {
  client.invoke(mbean,"getMBeanInfo",["javax.management.ObjectName"],callback);
} */ //could not get this to work; it "could not find method getMBeanInfo(javax.management.ObjectName)",
 //so I think this won't work because of mandatory first argument: it's looking IN the MBean object and not the server

//||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\\
//**********^v^v       *****      (                        )                  v^v^**********\\
//**********^v^v            *****(      SETUP___FUNCTIONS   )*****            v^v^**********\\
//**********^v^v                  (                        )      *****       v^v^**********\\
//||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\\

//queries nodes in cluster and selects target pods based on service/label names
async function get_services() {
  parameters = {
    'sleeptime' : 10000,
    'falkonryPortno' : 3000,
    'nameConditions' : ['drill', '!falkonry']
  };
  clients = [];
  client_names = []; client_pods = [];
  blacklist = {};

  try {
    //This section of code scans the kubernetes cluster and retrieves a list of all
    //pods and services; it then selects for the pods of interest below
    const client = new Client({ config: config.getInCluster() });
    await client.loadSpec();
    const service_promise = client.api.v1.services.get();
    const pods_promise = client.api.v1.pods.get();
    const service_list = await service_promise; const pods_list = await pods_promise;
    var pods = pods_list.body.items;
    var services = service_list.body.items;
    //we don't currently need the service list, I coded this part thinking we would
    //extract some other information about the service, and left it in in case
    //someone wants to do exactly that later.
    //https://raw.githubusercontent.com/godaddy/kubernetes-client/ea6725373b2a7a8ef9b79fc6aabd51101cd9ab84/docs/1.10.md
    for (var cand in services) {
      var servy = {}
      if (services[cand].metadata && services[cand].metadata.name) {
        servy.name = services[cand].metadata.name;
        //IMPORTANT: CURRENTLY, THIS IS OUR ONLY CRITERIA FOR SELECTION
        //if (servy.name.match('drill') || !servy.name.match('falkonry'))
        var veracity = false;
        var conditions = parameters['nameConditions'];
        for (var cond in conditions) {
          if (conditions[cond].match(/&!(.*)/)) { //AND (not X)
            veracity &= !(servy.name.match(RegExp.$1));
          } else if (conditions[cond].match(/&(.*)/)) { //AND X
            veracity &= (servy.name.match(RegExp.$1));
          } else if (conditions[cond].match(/!(.*)/)) { // not X
            if (veracity) break; //default OR, so short circuit if already true
            veracity = !(servy.name.match(RegExp.$1));
          } else {
            if (veracity) break;
            veracity = (servy.name.match(RegExp.$1));
          }
        }
        if (veracity) client_names.push(servy.name);
      }
      //later can get other stuff too, as needed: ports, selector, clusterIP
    }

    //only try to measure data if the Falkonry port is reported as open
    function falkonry_port_is_open(pod) {
      if (pod.spec && pod.spec.containers) {
        for (var con in pod.spec.containers) {
          var ports = pod.spec.containers[con].ports;
          for (var port in ports) {
            for (var key in ports[port]) {
              if (ports[port][key] == parameters['falkonryPortno']) {
                return true;
              }
            }
          }
        }
      }
      return false;
    }

    for (var proc in pods) {
      if (pods[proc].metadata && pods[proc].metadata.labels) {
        if ((client_names.indexOf(pods[proc].metadata.labels['name']) != -1)&&(falkonry_port_is_open(pods[proc]))) {
          client_pods.push([
              pods[proc].metadata.labels['name'],
              pods[proc].metadata.name,
              pods[proc].metadata.uid
            ]);
        }
      }
    }
    init();
  } catch (err) {
    console.log('kub-cli error: ', err);
  }
}
//create client/connection objects for pods found in get_services or passed in
//at the command line
function fill_clients() {
  
  //Make clients for cluster-scanned targets
  for (var cname in client_pods) {
    var args = {}; args['host'] = client_pods[cname][0];
    args['port'] = parameters['falkonryPortno'];
    _client = jmx.createClient(args);
    var cli = _client; cli.id = clients.length;
    clients.push(cli);
  }
  
  //Clients input via the command line
  //holdover; no reason to get rid of this, even if it's """deprecated"""
  for (var i = 2; i < process.argv.length; i++) {
    if (process.argv[i].match('^[--]')) {
      if (process.argv[i].match(/^[--](sleep|s)=(\d)+$/i)) {
        parameters['sleeptime'] = String.parseInt(RegExp.$2);
      } else if (process.argv[i].match(/^[--](port|p|P)=(\d)+$/i)) {
        parameters['falkonryPortno'] = String.parseInt(RegExp.$2);
      } else if (process.argv[i].match(/^[--](keyword|kw)=(.+)/i)) {
        parameters['nameConditions'] = (RegExp.$2).split(',');
      }
    } else {
      var args = {}; args['host'] = process.argv[i];
      args['port'] = parameters['falkonryPortno'];
      _client = jmx.createClient(args);
      var cli = _client; cli.id = clients.length;
      clients.push(cli);
      client_pods.push([process.argv[i], false, false]);
    }
  }
}

//||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\\
//**********^v^v       *****      (                        )                  v^v^**********\\
//**********^v^v            *****(      STRING___PARSING    )*****            v^v^**********\\
//**********^v^v                  (                        )      *****       v^v^**********\\
//||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\\

//Buffer_parse 1 and 2 are specialized parsing functions required for a certain MBean data
//return format; while standard beans returned this format during initial tests, none of the
//standard or nonstandard beans on the falkonry drill use this format (called CompositeDataSupport).
//To be safe, I have left this functionality, as well as a conditional during data collection
//that does a quick string parse on the data to see if it is in CompositeData format.
//buffer_parse_1 IS still required for attribute parsing (MBeanInfo data objects)

//gets rid of leading dot chain and replaces ( & ) with [ & ] where appropriate
function buffer_parse_1(instring) {
  var outstring = "";
  var buffer = "";
  var last_char = '_';
  var brackets = [];
  var pending = false;
  for (var i = 0; i < instring.length; i++) {
    if (instring[i] == '.')
      buffer = "";
    else {
      //MUST be an array if:
      if (instring[i] == '(') {
        //assumes arrays cannot be keys, only values
        if (last_char == '(') {
          outstring += buffer + '[';
          buffer = "";
          brackets.push('[');
        } else {};
      } else if (instring[i] == ')') {
        outstring += buffer;
        buffer = "";
        if (brackets.pop() == '[') {
          outstring += ']';
        } else outstring += ')';
      } else if (instring[i] == '}') {
        outstring += buffer + ')';
        buffer = "";
        brackets.pop();
      } else if (instring[i] == '{') {
        outstring += buffer + '(';
        buffer = "";
        brackets.push('(');
      } else if (instring[i] == ',' || instring[i] == '=' || instring[i] == ' ') {
        if (pending) {
          if (instring[i] == '=' || instring[i] == ' ') { //if space is the first thing that comes up, it is not
            brackets.push('(');       //an array, but an actual '(' in a string literal
            outstring += '(';
          } else {
            brackets.push('[');
            outstring += '[';
          }
          pending = false;
        }
        outstring += buffer + instring[i];
        buffer = "";
        //if we passed a '(': wait to see if it was an array or dict
        //this is determined by whether we see a ',' or '=' first.
      } else if (last_char == '(') {
        outstring += buffer;
        pending = true;
        buffer = instring[i];
      } else buffer += instring[i];
    }

    last_char = instring[i];
  }

  return outstring;
}
//build JSON from remaining output
function buffer_parse_2(instring, podId="") {
  var response = {};
  var buffer = "";
  var key_stage = true; //false=value_stage
  var brackets = ['{'];
  var key_hierarchy = [];
  var index_hierarchy = [];
  if (podId != "") {
    response['podId'] = podId;
  }
  var temp = response;
  var temp2 = temp;

  for (var i = 0; i < instring.length; i++) {

    if (instring[i] == '=') {//assume we never hit this in val stage
      //new "most recent key"; might need to open new dict, might not
      key_hierarchy.push(buffer);
      buffer = "";
      key_stage = false;
    } else if (instring[i] == '(' || instring[i] == '{') {
        //don't need "names"/functions, in this case, SimpleType({})
        if (key_stage && (buffer != "")) buffer = "";
        else {
          //"open" new dict and "enter" it.
          if (brackets.slice(-1) == '{') {
            temp[key_hierarchy.slice(-1)] = {};
            temp2 = temp[key_hierarchy.slice(-1)];
          } else {
            temp[index_hierarchy.slice(-1)] = {};
            temp2 = temp[index_hierarchy.slice(-1)];
          }
          temp = temp2;
          brackets.push("{");
          buffer = "";
          key_stage = true;
        }
    } else if (instring[i] == ')' || instring[i] == '}') {
        //add last value, done with key
        if (!key_stage) {
          temp[key_hierarchy.pop()] = buffer;
          buffer = "";
        }
        key_stage = true;
        //"close" this dict and walk out from root to 1 level above
        brackets.pop();
        //key_hierarchy.pop()
        temp = response;
        var j = 0; var k = 0;
        for (var b = 0; b < brackets.length - 1; b++) {
          if (brackets[b] == '{') {
            temp2 = temp[key_hierarchy[j]];
            temp = temp2; j++;
          } else if (brackets[b] == '[') {
            temp2 = temp[index_hierarchy[k]];
            temp = temp2; k++;
          }
        }
    } else if (instring[i] == ',') {
      if (brackets.slice(-1) == '[') {
        //array of strings/literals
        index_hierarchy[index_hierarchy.length - 1] += 1;
        if (key_stage) {
          if (buffer != "") temp.push(buffer);
          buffer = "";
        } else {
          //I guess with my design paradigm, should never get here
        }
      } else {
        //write the value for the most recent key & get rid of that key
        if (buffer != "") {
          temp[key_hierarchy.pop()] = buffer;
          buffer = "";
        }
        //look for new key
        key_stage = true;
      }
    } else if (instring[i] == '[') {
      if (brackets.slice(-1) == '{') {
        temp[key_hierarchy.slice(-1)] = [];
        temp2 =  temp[key_hierarchy.slice(-1)];
      } else {
        temp[index_hierarchy.slice(-1)] = [];
        temp2 =  temp[index_hierarchy.slice(-1)];
      }
      temp = temp2;
      buffer = "";
      brackets.push('[');
      index_hierarchy.push(0);
      key_stage = true;
    } else if (instring[i] == ']') {
      brackets.pop();
      if (key_stage && (buffer != "")) temp.push(buffer);
      index_hierarchy.pop();
    } else {
      //buildup current key/value
      buffer += instring[i];
    }
  }

  return response;
}
//shallow scan only; this is the closest to being "hard-coded" (due to bizarre use of '[' on their part)
//But the point is, this function will only capture attribute properties (ie values in the key-value pairs)
//that are strings, it will not convert arrays or dictionaries
function parse_attribute_list(instring) {
  var attributes = [];
  var buffer = "";
  var phase = 0;
  var buffered_brackets = 0;
  var curr = {};
  var _last_key = "";
  for (var g = 0; g < instring.length; g++) {
    if (instring[g] == '[') {
      if (phase == 1) { //we have seen "attributes"
        if (buffer == "MBeanAttributeInfo") { //don't want OpenMBeanAttributeInfoSupport type/info; that
              //info is redundant thanks to the parse of the GET results; custom beans don't even have this type
          phase = 2; //we are in the attributes list
        } else { //unfortunately, the transform in buffer_parser_1 where we change ( to [ sometimes to indicate an array
            //means we have to do this so that we don't "close" the attribute list too early because of an inserted ']'
          buffered_brackets++;
        }
      } //else: nothing special
      buffer = "";
    } else if (instring[g] == ']') {
      if (phase == 2) { //cap off one attr
        if (_last_key != "") {
          curr[_last_key] = buffer;
        }
        phase = 1;
        attributes.push(curr);
      } else if (phase == 1) { //last attr in list
        if (buffered_brackets == 0)
          return attributes;
        else
          buffered_brackets--;
      }
      buffer = ""; curr = {}; _last_key = "";
    } else if (instring[g] == '=') {
      if (buffer == "attributes") {
        phase = 1;
        buffer = "";
      } else if (phase == 2) {
        _last_key = buffer;
        buffer = "";
      }
    } else if (instring[g] == ',') {
      if (phase < 2) {
        //do nothing
      } else if (_last_key != "") {
        curr[_last_key] = buffer;
      }
      buffer = ""; _last_key = "";
    } else if (instring[g] == ' ') { //ignore spaces, unless in "description" key
      if (_last_key != "") buffer += instring[g];
    } else {
      buffer += instring[g];
    }
  }
  return attributes; //should not get here
}

//||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\\
//**********^v^v       *****      (                        )                  v^v^**********\\
//**********^v^v            *****(      DATA___TRANSFORMS   )*****            v^v^**********\\
//**********^v^v                  (                        )      *****       v^v^**********\\
//||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\\

//If we ran into any "Unsupported" Attributes, remove them from our 'parameters' object
//before we begin querying off of it
function remove_blacklisted_attributes() {
  //for every client
  for (var _cli in clients) {
    //for every mbean in that client
    for (var mb in parameters[_cli]) {
      //if any blacklisted words are associated with this client and bean
      if (blacklist[_cli] && (blacklist[_cli][mb])) {
        var new_list = [];
        //for every attribute in that mbean
        for (var atb in parameters[_cli][mb]['attributes']) {
          //for every blacklisted attribute name associated with that client and mbean
          for (var listed in blacklist[_cli][mb]) {
            //if the attribute containst the blacklisted word/property as a substring, remove it
            if (parameters[_cli][mb]['attributes'][atb].name.match(blacklist[_cli][mb][listed])) {
              //don't add to list
            } else { new_list.push(parameters[_cli][mb]["attributes"][atb]); }
          }
        }
        parameters[_cli][mb]['attributes'] = new_list;
      }
    }
  }
}

//recursive function that performs the following condensation: in drill, for example, the following are
//separate attributes: "metrics:name=heap.committed", "metrics:name=heap.init", "metrics:name=heap.max", etc;
//This will merge them so that instead of a separate dict for each, you get "metrics:name=heap" : {
// "init" : {"Value" : --- }, "max" : {"Value" : ---}, ...  
//}
function final_condense(input) {
  function ref_condense(injson) {
    var temp = injson;
    var temp2 = {};
    var buffer = "";
    for (var name in temp) {
      temp = injson;
      var parts = name.split('.');
      for (var o = 0; ; o++) {
        if (o+1 == parts.length) {
          temp2 = JSON.parse(JSON.stringify(injson[name])); //deep clone
          delete injson[name];
          temp[parts[o]] = temp2;
          break;
        }
        if (!temp[parts[o]]) temp[parts[o]] = {};
        temp = temp[parts[o]];
      }
    }
  }

  var _temp = input;
  for (var branch in _temp) {
    if (_temp[branch] != null && (typeof _temp[branch] === 'object')) {
      final_condense(_temp[branch]);
    }
  } //leaf-first recursion
  ref_condense(_temp);
}

//||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\\
//**********^v^v       *****      (                        )                  v^v^**********\\
//**********^v^v            *****(      DATA___QUERYING     )*****            v^v^**********\\
//**********^v^v                  (                        )      *****       v^v^**********\\
//||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\\

//Fetches the client's list of MBeans, creates and shedules tasks to retrieve each
//MBean's attribute list, and then passes control on to the next phase: removal
//of blacklisted attributes
function get_mbean_info(index) {
  blacklist_candidates = [];
  if (!parameters[index]) parameters[index] = {};
  //FOR EACH CLIENT, GET ITS COMPLETE LIST OF MBEANS
  clients[index].listMBeans(function(mbeans) {
    var bean_tasks = [];
    var get_attr = function(mbean) {
      return function(callback) { get_attributes_list(mbean, index, callback); }
    };
    mbeans.forEach(function(mbean) {
      if (!parameters[index][mbean]) parameters[index][mbean] = {};
      bean_tasks.push(get_attr(mbean));
    });
    async.parallel(bean_tasks, function(err, results) {
      decide_blacklist(index);
    });
  });
}
//Task to retrieve the attribute list of a given MBean; in this version,
//checking whether the attributes in this list are valid/supported is deferred:
//a synchronous function moves potentially offensive attributes into a list that
//will be checked in batch later.
function get_attributes_list(mbean, index, callback) {
  var ind = index; var mb = mbean;
  //FOR EACH MBEAN, GET COMPLETE LIST OF QUERYABLE ATTRIBUTES
  listAttributes(clients[ind], mb, function(data)  {
    if (data) {
      var attr_list = parse_attribute_list(buffer_parse_1(data.toString()));
      check_attributes_supported(attr_list, mb, ind); //sync
      parameters[ind][mb]['attributes'] = attr_list;
    }
    callback(null, data);
  });
}
//checks attributes list for "Supported" fields; if it finds one, it pushes it onto a list
//so that we can batch check the values later; the check used to be done here, but parallelism
//was sacrificed for code simplicity/modularity
function check_attributes_supported(attr_list, key, index) {
  for (var a in attr_list) {
    if (!attr_list[a].name) { console.log("Error: attribute "+a+" of MBean "+key+" has no 'name' field."); continue; }
    //NOT ALL LISTED ATTRIBUTES ARE TRULY QUERYABLE; IF A "<MBEANNAME>Supported" ATTRIBUTE EXISTS,
    //WE MUST VERIFY THAT THIS MBEAN IS SUPPORTED ON THIS CLIENT, AND ADJUST ACCORDINGLY
    var _mat = attr_list[a]['name'].match(/(.*)Supported/i);//(/(.*)supported/i);
    if (_mat != null) blacklist_candidates.push(index.toString()+"?"+key+'?'+_mat[1]);//use some char that cannot appear as part of the MBean or service name
  }
}
//create and schedule tasks to check the values of <MBEAN>Supported attributes and decide
//what attribute keywords will be pushing into the final blacklist; when all tasks have
//completed, it calls the next phase: a synchronous call to "remove_blacklisted_attributes"
//which purges blacklisted attributes from the parameter tree, and then begins the
//asynchronous data collection phase
function decide_blacklist(index) {
  var tasks = [];
  var get_word = function(word) {
    return function (callback1) {
      do_blacklist_check(word, callback1);
    };
  };
  blacklist_candidates.forEach(function(word) {
    tasks.push(get_word(word));
  });
  async.parallel(tasks, function(err, results) {
    remove_blacklisted_attributes(); //sync
    get_mbean_data(index);
  });
}
//An individual, asynchronous task to determine the value of an <MBEAN>Supported
//attribute and add it to the final blacklist as needed
function do_blacklist_check(word, callback) {
  var _args = word.split('?');
  var index = _args[0]; var mbean = _args[1]; var keyword = _args[2];
  clients[index].getAttribute(mbean, keyword+'Supported', function(data) {
    if (data === true) { }
    else {
      if (!blacklist[index]) blacklist[index] = {};
      if (!blacklist[index][mbean]) blacklist[index][mbean] = [keyword];
      else blacklist[index][mbean].push(keyword);
    }
    callback(null, data);
  });
}
//Data collection phase; create and schedule tasks to fetch data for each
//attribute of each MBean of the client; when each task finishes, we move on
//to the final phase: we collect a timestamp, format and print the data, and
//close the connection
function get_mbean_data(cli) {
  var temp_dict = {};
  var data_tasks = [];
  temp_dict['pod_label_name'] = client_pods[cli][0];
  //clients passed in at the command-line will not have these fields
  if (client_pods[cli][1]) temp_dict['pod_full_name'] = client_pods[cli][1];
  if (client_pods[cli][2]) temp_dict['pod_uid'] = client_pods[cli][2];

  for (var mbean in parameters[cli]) {
    temp_dict[mbean] = {};
    for (var attr in parameters[cli][mbean]['attributes']) {
      curr_name = parameters[cli][mbean]['attributes'][attr].name;
      var toDo = function(ind, mbean, name) {
        return function(callback) {
          process_data(ind, temp_dict[mbean], mbean, name, callback);
        };
      }
      data_tasks.push(toDo(cli,mbean,curr_name));
    }
  }

  async.parallel(data_tasks, function(err, results) {
    //coallesce related fields
    final_condense(temp_dict);
    temp_dict['print time'] =  new Date().getTime();
    console.log(JSON.stringify(temp_dict));
    //we are finished with this client, disconnect it
    clients[cli].disconnect();
  });
}
//individual task to retrieve a set of data associated with a particular mbean and attribute;
//may or may not require special parsing
function process_data(cli, dict, mbean, attribute_name, callback) {
  clients[cli].getAttribute(mbean, attribute_name, function (data) {
    if (data.toString().match(/CompositeDataSupport(.*)compositeType=javax\.management.openmbean.CompositeType/i))
      dict[attribute_name] = buffer_parse_2(buffer_parse_1(data.toString()));
    else dict[attribute_name] = data.toString();
    callback(null, data);
  });
}

//create clients and initialize listener functions; these listener functions are the only
//hooks node-jmx provides into the client
function init() {

  fill_clients();

  //SETUP CONNECTION CALLBACKS FOR ALL CLIENTS
  for (var cli in clients) {

    clients[cli].on("connect", function() {
      var index = this.id;
      get_mbean_info(index);
    });

    clients[cli].on("error", function(err) {
      console.log("Error on: "+client_pods[this.id][0].toString());
      console.log(err.toString());
      //https://community.hortonworks.com/content/supportkb/182962/javalangunsupportedoperationexception-collectionus.html May be a way to actually fix/get data?
      if (err.toString().match('javax.management.RuntimeMBeanException: java.lang.UnsupportedOperationException: (.*) is not supported'))
        console.log("MBean "+RegExp.$1+" is not supported.");
    });

    //if we disconnect, we try the next client
    clients[cli].on("disconnect", function() {
      clients.pop();
      setTimeout(callNext, parameters['sleeptime']);
    });
  }

  //start querying
  callNext();
}

//connect/begin data collection for the next client; if none exists (list is empty),
//we start again from the beginning
function callNext() {
  if (clients.length === 0) setTimeout(get_services, parameters['sleeptime']);
  else {
    clients[clients.length - 1].connect();
  }
}

//start the process
get_services();
