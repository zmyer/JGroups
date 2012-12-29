package org.jgroups.protocols.rules;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.BoundedList;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Tuple;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Protocol which supervises other protocols. Essentially a rule engine, where rule conditions are checked periodically,
 * triggering (optional) actions. Example: start failure detection in FD is it isn't running<p/>
 * https://issues.jboss.org/browse/JGRP-1557
 * @author Bela Ban
 * @since  3.3
 * todo: add event based rule triggering
 */
@MBean(description="Supervises the running stack, taking corrective actions if necessary")
public class SUPERVISOR extends Protocol {
    protected Address                     local_addr;

    protected volatile View               view;

    // The timer used to run the rules on
    protected TimeScheduler               timer;

    // The last 50 executions, can be retrieved with executions()
    protected final BoundedList<String>   executions=new BoundedList<String>(50);

    // hashmap of rules, keys are rule names and values futures to the rules
    protected final Map<String,Tuple<Rule,Future<?>>> rules=new HashMap<String,Tuple<Rule,Future<?>>>();


    @ManagedAttribute(description="Location of an XML file listing the rules to be installed")
    protected String                      rule_config;

    public Address getLocalAddress()      {return local_addr;}
    public View    getView()              {return view;}

    @ManagedOperation(description="Prints the last N conditions that triggered a rule action")
    public String executions() {
        StringBuilder sb=new StringBuilder();
        for(String execution: executions)
            sb.append(execution + "\n");
        return sb.toString();
    }

    public void addCondition(String cond) {
        executions.add(new Date() + ": " + cond);
    }

    @ManagedOperation(description="Prints all currently installed rules")
    public String dumpRules() {
        StringBuilder sb=new StringBuilder();
        for(Tuple<Rule,Future<?>> tuple: rules.values()) {
            Rule rule=tuple.getVal1();
            sb.append(rule.name() + ": " + rule.description() + "\n");
        }
        return sb.toString();
    }

    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
        if(timer == null)
            throw new IllegalStateException("timer not found");
    }

    public void destroy() {
        for(Tuple<Rule,Future<?>> tuple: rules.values())
            tuple.getVal2().cancel(true);
        rules.clear();
        super.destroy();
    }

    /**
     * Installs a new rule
     * @param interval Number of ms between executions of the rule
     * @param rule The rule
     */
    public void installRule(long interval, Rule rule) {
        rule.supervisor(this).log(log).init();
        Future<?> future=timer.scheduleAtFixedRate(rule, interval, interval, TimeUnit.MILLISECONDS);
        Tuple<Rule,Future<?>> existing=rules.put(rule.name(),new Tuple<Rule,Future<?>>(rule, future));
        if(existing != null)
            existing.getVal2().cancel(true);
    }

    public void uninstallRule(String name) {
        if(name != null) {
            Tuple<Rule,Future<?>> tuple=rules.remove(name);
            if(tuple != null) {
                tuple.getVal2().cancel(true);
                tuple.getVal1().destroy();
            }
        }
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    protected void handleView(View view) {
        this.view=view;
    }
}
