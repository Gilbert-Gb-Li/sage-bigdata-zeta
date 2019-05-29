/*_############################################################################
  _##
  _##  SnmpWalk.java
  _##
  _##  SNMP4J-SMI
  _##  ----------------------------------
  _##  Copyright (C) 2012-2013 Frank Fock
  _##
  _##  Use of this software is subject to the license agreement you received
  _##  with this software in the "legal" folder and which can be downloaded
  _##  from http://www.snmp4j.com/smi/LICENSE.txt
  _##
  _##  Last updated: Tue Mar 12 01:01:26     2013
  _##
  _##########################################################################*/

import com.snmp4j.smi.CompilationResult;
import com.snmp4j.smi.SmiError;
import com.snmp4j.smi.SmiManager;
import com.snmp4j.smi.util.MemRepositoryDriver;
import com.snmp4j.smi.version.VersionInfo;
import org.snmp4j.*;
import org.snmp4j.mp.*;
import org.snmp4j.security.SecurityModel;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.smi.*;
import org.snmp4j.transport.DefaultTcpTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.Exception;
import java.lang.String;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The {@link SnmpWalk} sample program implements a classic sub-tree walk using
 * SNMP4J and SNMP4J-SMI.
 * <p/>
 * To compile this example run
 * <pre>
 *   javac -cp ../snmp4j-smi.jar;../lib/snmp4j-2.2.0.jar SnmpWalk.java
 * </pre>
 * in the example directory.
 * <p/>
 * Then run it with the following command line to get usage information:
 * <pre>
 *   java -cp ../snmp4j-smi.jar;../lib/snmp4j-2.2.0.jar;. SnmpWalk help all
 * </pre>
 *
 * @version 1.0
 * @author Frank Fock
 */
public class SnmpWalk {

  private static final String VERSION = VersionInfo.getVersion();

  private static final String MIB_OPTIONS =
      "+M[s] +m[s] ";
  private static final String V3_OPTIONS =
      "+a[s<MD5|SHA>] +A[s] +bc[i{=0}] +e[x] +E[x] "+
          "+y[s<DES|3DES|AES128|AES192|AES256>] +Y[s] +u[s] +l[x] +n[s] ";
  private static final String SNMP_OPTIONS =
      "+c[s] +r[i{=1}] +t[i{=5000}] +v[s{=3}<1|2c|3>] +Ors[i{=65535}] ";
  private static final String BULK_OPTIONS =
      "-Cr[i{=10}] -Cn[i{=0}] ";

  private static final String ADDRESS_PARAMETER =
      "#address[s<((udp|tcp):)?.*[/[0-9]+]?>] ";

  private static final String OID_PARAMETER =
      "#OID[s<([a-zA-Z\\-0-9]*:)?[0-9a-zA-Z\\-\\.#]*(=(\\{[iusxdnotab]\\})?.*)?>] ";

  private static final String ALL_OPTIONS = MIB_OPTIONS +
      V3_OPTIONS + SNMP_OPTIONS + BULK_OPTIONS;

  private static final String[][] COMMANDS = {
    {
      "walk",
      SNMP_OPTIONS + V3_OPTIONS + BULK_OPTIONS + MIB_OPTIONS,
        "#command[s<walk>] " + ADDRESS_PARAMETER + OID_PARAMETER
    }, {
      "mib",
      MIB_OPTIONS,
      "#command[s<mib>] #mode[s<add|del|list>] +file[s]"
    }, {
      "help",
      "",
      "#command[s<help>] +subject[s<all|mib|walk>]"
    }
  };

  private static int stdoutWidth = 79;
  private static int[] tabs = null;

  private static ResourceBundle help = ResourceBundle.getBundle("snmpWalkHelp");
  private String command;
  private Target target;
  private PDUFactory pduFactory;
  private Snmp snmp;
  private SmiManager smiManager;
  private Map<String,Object> commandLineParameters;

  private PrintStream out = System.out;
  private PrintStream err = System.err;

  @SuppressWarnings(value = "unchecked")
  public SnmpWalk(String command, Map<String,Object> commandLineParameters) throws IOException {

    this.command = command;
    this.commandLineParameters = commandLineParameters;
    SnmpConfigurator snmpConfig = new SnmpConfigurator();
    target = snmpConfig.getTarget(commandLineParameters);

    TransportMapping<? extends SMIAddress> transport;
    if (target.getAddress() instanceof TcpAddress) {
      transport = new DefaultTcpTransportMapping();
    }
    else {
      transport = new DefaultUdpTransportMapping();
    }
    // Set the default counter listener to return proper USM and MP error
    // counters.
    configureSnmp(snmpConfig, transport, commandLineParameters);
    snmp.listen();

    smiManager = configureSnmp4jSMI(commandLineParameters);

    pduFactory = snmpConfig.getPDUFactory(commandLineParameters);

  }

  @SuppressWarnings(value = "unchecked")
  public void run() throws IOException {
    if ("walk".equals(command)) {
      PDU request = pduFactory.createPDU(target);
      OID rootOID = new OID((String) ArgumentParser.getFirstValue((List) commandLineParameters.get("OID")));
      request.add(new VariableBinding(rootOID));
      walk(snmp, request, target);
    }
    else if ("mib".equals(command)) {
      updateRepository(out);
    }
  }

  private PDU walk(Snmp snmp, PDU request, Target target) throws
      IOException {
    try {
      request.setNonRepeaters(0);
    }
    catch (UnsupportedOperationException uex) {
      // ignore
    }
    OID rootOID = request.get(0).getOid();
    PDU response = null;
    final WalkCounts counts = new WalkCounts();
    final long startTime = System.currentTimeMillis();
    TreeUtils treeUtils = new TreeUtils(snmp, pduFactory);
    TreeListener treeListener = new TreeListener() {
      private boolean finished = false;
      public boolean next(TreeEvent e) {
        counts.requests++;
        if (e.getVariableBindings() != null) {
          VariableBinding[] vbs = e.getVariableBindings();
          counts.objects += vbs.length;
          for (VariableBinding vb : vbs) {
            out.println(vb.toString());
          }
        }
        return true;
      }

      public void finished(TreeEvent e) {
        if ((e.getVariableBindings() != null) &&
            (e.getVariableBindings().length > 0)) {
          next(e);
        }
        err.println();
        err.println("Total requests sent:    " + counts.requests);
        err.println("Total objects received: " + counts.objects);
        err.println("Total walk time:        " +
            (System.currentTimeMillis() - startTime) +
            " milliseconds");
        if (e.isError()) {
          err.println("The following error occurred during walk:");
          err.println(e.getErrorMessage());
          //e.getException().printStackTrace();
        }
        finished = true;
        synchronized (this) {
          this.notify();
        }
      }

      public boolean isFinished() {
        return finished;
      }
    };
    synchronized (treeListener) {
      treeUtils.getSubtree(target, rootOID, null, treeListener);
      try {
        treeListener.wait();
      }
      catch (InterruptedException ex) {
        err.println("Tree retrieval interrupted: " + ex.getMessage());
      }
    }
    return response;
  }

  private void updateRepository(PrintStream out) throws IOException {
    String mode = (String) ArgumentParser.getValue(commandLineParameters, "mode", 0);
    String fname = (String) ArgumentParser.getValue(commandLineParameters, "file", 0);
    if ("add".equals(mode)) {
      if (fname == null) {
        out.println("Please specify a MIB file or directory name.");
      }
      else {
        File f = new File(fname);
        if (f.exists() && f.canRead()) {
          File[] files;
          if (f.isDirectory()) {
            files = f.listFiles();
          }
          else {
            files = new File[1];
            files[0] = f;
          }
          List<CompilationResult> results = smiManager.compile(files, null, true, true, false);
          int ok = 0;
          for (CompilationResult result : results) {
            if (result.getSmiErrorList() == null) {
              ok += result.getModuleNames().size();
            }
          }
          out.println("" + f + " contains ");
          out.println(" " + ok + " syntactically correct MIB modules and");
          out.println(" " + (results.size() - ok) +
              " MIB modules with errors.");
          String lastFile = null;
          for (CompilationResult result : results) {
            List<SmiError> smiErrors = result.getSmiErrorList();
            String n = result.getFileName();
            n = n.substring(n.lastIndexOf('/') + 1);
            if ((lastFile == null) || (!lastFile.equals(n))) {
              out.println("------ " + n + " ------");
              lastFile = n;
            }
            if (smiErrors != null) {
              for (int j = 0; j < smiErrors.size(); j++) {
                SmiError error = smiErrors.get(j);
                String txt = n + " #" + (j + 1) + ": " + error.getMessage();
                out.println(txt);
              }
            }
            else {
              String txt = n + ": " + "OK";
              out.println(txt);
            }
          }
        }
        else {
          out.println("Cannot access MIB file '" + fname + "'");
        }
      }
    }
    else if ("del".equals(mode)) {
      if (fname == null) {
        out.println("Please specify a MIB module name.");
      }
      else {
        List<String> dependentMibModules = smiManager.deleteModule(fname, false);
        if (dependentMibModules != null && dependentMibModules.size()>0) {
          out.println("MIB module '" + fname + "' not deleted, because the MIB modules " + dependentMibModules + " depend on it.");
          out.println("Please delete those MIB modules first, before trying again.");
        }
      }
    }
    else if ("list".equals(mode)) {
      String[] mibModuleNames = smiManager.listModules();
      if ((mibModuleNames == null) || (mibModuleNames.length == 0)) {
        out.println("The MIB repository is invalid or does not contain any MIB modules.");
      }
      else {
        for (String mibModuleName : mibModuleNames) {
          out.println(mibModuleName);
        }
      }
    }
  }


  private static SmiManager configureSnmp4jSMI(Map<String,Object> commandLineParameters) throws IOException {
    SmiManager smiManager;
    // Define the MIB repository
    if (commandLineParameters.containsKey("M")) {
      @SuppressWarnings(value = "unchecked")
      String f = (String) ArgumentParser.getFirstValue((List) commandLineParameters.get("M"));
      smiManager = new SmiManager(null, new File(f));
    }
    else {
      smiManager = new SmiManager(null, new MemRepositoryDriver());
    }
    // Load MIB modules from the repository
    if (commandLineParameters.containsKey("m")) {
      @SuppressWarnings(value = "unchecked")
      List<String> modulePatterns = (List<String>) commandLineParameters.get("m");
      loadModules(smiManager, modulePatterns);
    }
    // Here the formatting with MIB information is finally configured in SNMP4J:
    SNMP4JSettings.setOIDTextFormat(smiManager);
    SNMP4JSettings.setVariableTextFormat(smiManager);
    return smiManager;
  }

  private static void loadModules(SmiManager smiManager, List<String> modulePatterns) throws IOException {
    String[] availableModules = smiManager.listModules();
    for (String modulePattern : modulePatterns) {
      Pattern p = Pattern.compile(modulePattern);
      for (String availableModule : availableModules) {
        Matcher m = p.matcher(availableModule);
        if (m.matches()) {
          smiManager.loadModule(availableModule);
        }
      }
    }
  }

  @SuppressWarnings(value = "unchecked")
  private void configureSnmp(SnmpConfigurator snmpConfig, TransportMapping<? extends SMIAddress> transport,
                             Map<String,Object> settings) {
    CounterSupport.getInstance().addCounterListener(new
        DefaultCounterListener());
    MessageDispatcher bmd = new MessageDispatcherImpl();
    bmd.addMessageProcessingModel(new MPv2c());
    bmd.addMessageProcessingModel(new MPv1());
    bmd.addMessageProcessingModel(new MPv3());
    SecurityProtocols.getInstance().addDefaultProtocols();
    snmp = new Snmp(bmd, transport);
    if ((settings.containsKey("v")) &&
        ("3".equals(ArgumentParser.getValue(settings, "v", 0)))) {
      MPv3 mpv3 = (MPv3) snmp.getMessageProcessingModel(MPv3.ID);
      if (target.getSecurityModel() == SecurityModel.SECURITY_MODEL_USM) {
        SecurityModels.getInstance().addSecurityModel(
            new USM(SecurityProtocols.getInstance(),
                new OctetString(mpv3.getLocalEngineID()),
                0));
      }
    }
    snmpConfig.configure(snmp, settings);
  }

  @SuppressWarnings(value = "unchecked")
  public static void main(String[] args) {
    Map<String,Object> commandLineParameters;
    try {
      String[] commandSet =
          ArgumentParser.selectCommand(args, ALL_OPTIONS, COMMANDS);
      if (commandSet == null) {
        printUsage();
        System.exit(2);
      }
      ArgumentParser parser =
          new ArgumentParser(commandSet[1], commandSet[2]);
      commandLineParameters = (Map<String,Object>) parser.parse(args);
      String command =
          (String) ArgumentParser.getValue(commandLineParameters, "command", 0);
      if ("help".equals(command)) {
        String helpCmd =
            (String) ArgumentParser.getValue(commandLineParameters, "subject", 0);
        System.out.println(help("", helpCmd , true, true));
        System.exit(0);
      }
      else {
        SnmpWalk walker = new SnmpWalk(command, commandLineParameters);
        walker.run();
        System.exit(0);
      }
      printUsage();
      System.exit(2);
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
  }


  public static String help(String prefix, String command,
                     boolean listOptionsDetails, boolean withDescription) {
    if (prefix == null) {
      prefix = "";
    }
    StringBuffer buf = new StringBuffer();
    if ((command == null) || ("all".equals(command))) {
      String usage = help.getString("usage.text");
      buf.append(MessageFormat.format(usage, new Object[]{VERSION}));
      TreeMap<String,ArgumentParser.ArgumentFormat> options = new TreeMap<String,ArgumentParser.ArgumentFormat>();
      TreeMap<String,String[]> commands = new TreeMap<String,String[]>();
      for (String[] COMMAND : COMMANDS) {
        String c = COMMAND[0];
        String[] format = selectFormat(c);
        commands.put(c, format);
      }
      for (Map.Entry<String,String[]> e : commands.entrySet()) {
        String c = e.getKey();
        String[] format = e.getValue();
        ArgumentParser p = new ArgumentParser(format[0], format[1]);
        Map<String,ArgumentParser.ArgumentFormat> o = p.getOptionFormat();
        options.putAll(o);
        buf.append(c).append(":\n");
        buf.append(help("", c, false, (command != null)));
        buf.append('\n');
      }
      buf.append("\n\nOPTIONS:\n");
      optionDetailList(getTabPosition(0), prefix, buf, options);
    }
    else {
      String syn = help.getString("command.syn."+prefix+command);
      String des = help.getString("command.des."+prefix+command);
      if (syn != null && des != null) {
        String line = "";
        if (!"".equals(syn)) {
          line += tab(0, 0, 0) + syn;
          line += (withDescription) ? "\n\n" : "\n";
        }
        if (withDescription) {
          int firstLineIndent = 0;
          if (line.length() >= getTabPosition(0))  {
            line += '\n';
            firstLineIndent = getTabPosition(0);
          }
          line += format(getTabPosition(0), des, 0, firstLineIndent);
        }
        buf.append(line);
        buf.append('\n');
      }
      try {
        String subcmd = help.getString("command.sub." + prefix + command);
        if (subcmd != null) {
          String[] subcmds = subcmd.split(",");
          for (int i = 0; i < subcmds.length; i++) {
            if (i == 0) {
              buf.append("\n");
            }
            buf.append(spaces(getTabPosition(0)));
            buf.append(subcmds[i]).append((withDescription) ? ":\n" : "");
            buf.append(help(command + ".", subcmds[i], false, withDescription));
          }
        }
      }
      catch (MissingResourceException mrex) {
        // ignore
      }
      String od = "";
      if (listOptionsDetails) {
        od = options(getTabPosition(0), prefix, command);
      }
      else if ("".equals(prefix)) {
        od = optionList(getTabPosition(0), prefix, command);
      }
      if (od.length() > 0) {
        buf.append('\n');
        buf.append(spaces(getTabPosition(0)));
        buf.append("Options:\n");
        buf.append(od);
      }
      buf.append('\n');
    }
    return buf.toString();
  }

  private static String[] selectFormat(String command) {
    for (String[] COMMAND : COMMANDS) {
      if (COMMAND[0].equals(command)) {
        return new String[]{COMMAND[1], COMMAND[2]};
      }
    }
    return null;
  }

  public static String spaces(int n) {
    StringBuilder buf = new StringBuilder(n);
    for (int i=0; i<n; i++) {
      buf.append(' ');
    }
    return buf.toString();
  }

  private static String tab(int offset, int position, int tabNo) {
    StringBuilder buf = new StringBuilder();
    if (tabs == null) {
      String tabString = help.getString("tabs");
      String[] tabsArray = tabString.split(",");
      tabs = new int[tabsArray.length];
      for (int i=0; i<tabsArray.length; i++) {
        tabs[i] = Integer.parseInt(tabsArray[i]);
      }
    }
    int t = getTabPosition(tabNo);
    buf.append(spaces(Math.max(1, t + offset - position)));
    return buf.toString();
  }

  private static int getTabPosition(int tabNo) {
    return (tabNo < tabs.length) ?
        tabs[tabNo] : tabs[tabs.length-1] + (tabNo - tabs.length)*8;
  }

  private static String optionList(int indentation, String prefix, String command) {
    if (prefix != null) {
      return "";
    }
    StringBuilder buf = new StringBuilder();
    String[] format = selectFormat(command);
    ArgumentParser p = new ArgumentParser(format[0], format[1]);
    Map<String,ArgumentParser.ArgumentFormat> options = p.getOptionFormat();
    SortedMap<String,ArgumentParser.ArgumentFormat> soptions =
        new TreeMap<String,ArgumentParser.ArgumentFormat>(options);
    for (Iterator<String> it = soptions.keySet().iterator(); it.hasNext(); ) {
      String opt = it.next();
      buf.append("-").append(opt);
      if (it.hasNext()) {
        buf.append(", ");
      }
    }
    return format(indentation, buf.toString(), 0, 0);
  }

  private static String options(int indentation, String prefix, String command) {
    StringBuffer buf = new StringBuffer();
    String[] format = selectFormat(command);
    ArgumentParser p = new ArgumentParser(format[0], format[1]);
    Map<String,ArgumentParser.ArgumentFormat> options = p.getOptionFormat();
    SortedMap<String,ArgumentParser.ArgumentFormat> soptions =
        new TreeMap<String,ArgumentParser.ArgumentFormat>(options);
    optionDetailList(indentation, prefix, buf, soptions);
    return buf.toString();
  }

  private static void optionDetailList(int indentation, String prefix,
                                       StringBuffer buf, SortedMap<String,ArgumentParser.ArgumentFormat> soptions) {
    for (Object o1 : soptions.keySet()) {
      String opt = (String) o1;
      String o = spaces(indentation) + "-" + opt;
      String optSyn = help.getString("options.syn." + opt);
      o += tab(indentation, o.length(), 2);
      o += optSyn;
      buf.append(prefix);
      buf.append(o);
      String optDesc = help.getString("options.des." + opt);
      buf.append(format(getTabPosition(3), optDesc, 3, o.length()));
      buf.append('\n');
    }
  }

  private static String format(int indentation, String s, int tabNo,
                               int firstLineOffset) {
    StringTokenizer st = new StringTokenizer(s, "\t\n", true);
    StringBuilder buf = new StringBuilder();
    int lineLength = firstLineOffset;
    boolean firstLine = true;
    while (st.hasMoreTokens()) {
      String t = st.nextToken();
      if ("\t".equals(t)) {
        String spaces = tab(indentation, lineLength, tabNo++);
        buf.append(spaces);
        lineLength += spaces.length();
      }
      else if ("\n".equals(t)) {
        tabNo = 0;
        firstLine = false;
        lineLength = 0;
        buf.append("\n");
      }
      else {
        if ((firstLineOffset > 0) && firstLine) {
          String l = tab(0, firstLineOffset, tabNo);
          buf.append(l);
          lineLength += l.length();
        }
        else if ((lineLength == 0) && (indentation > 0)) {
          String l = spaces(indentation);
          buf.append(l);
          lineLength += l.length();
        }
        buf.append(wrap(indentation, t, stdoutWidth, lineLength));
        lineLength += t.length();
      }
    }
    return buf.toString();
  }

  private static String wrap(int offset, String s, int maxLineLength,
                             int firstLineOffset) {
    StringTokenizer st = new StringTokenizer(s, " ", true);
    StringBuilder buf = new StringBuilder(s.length());
    int ll = firstLineOffset;
    while (st.hasMoreTokens()) {
      String t = st.nextToken();
      if (ll+t.length() > maxLineLength) {
        buf.append('\n');
        if (offset > 0) {
          buf.append(spaces(offset));
        }
        if (!" ".equals(t)) {
          buf.append(t);
          ll = t.length() + offset;
        }
        else {
          ll = offset;
        }
      }
      else {
        buf.append(t);
        ll += t.length();
      }
    }
    return buf.toString();
  }

  private static void printUsage() {
    System.out.println(help("", "all", true, true));
  }

  class WalkCounts {
    public int requests;
    public int objects;
  }

}
