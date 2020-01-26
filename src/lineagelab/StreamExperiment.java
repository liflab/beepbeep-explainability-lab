/*
    A benchmark for data lineage in BeepBeep 3
    Copyright (C) 2017-2020 Laboratoire d'informatique formelle

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package lineagelab;

import ca.uqac.lif.azrael.PrintException;
import ca.uqac.lif.azrael.size.SizePrinter;
import ca.uqac.lif.cep.Connector;
import ca.uqac.lif.cep.EventTracker;
import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.Pullable;
import ca.uqac.lif.cep.Pushable;
import ca.uqac.lif.cep.tmf.BlackHole;
import ca.uqac.lif.json.JsonFalse;
import ca.uqac.lif.json.JsonList;
import ca.uqac.lif.json.JsonNumber;
import ca.uqac.lif.json.JsonTrue;
import ca.uqac.lif.labpal.Experiment;
import ca.uqac.lif.labpal.ExperimentException;
import lineagelab.source.BoundedSource;

/**
 * Experiment that connects a source to a processor and measures its
 * throughput
 */
public class StreamExperiment<T> extends Experiment
{
  public static transient final String TOOL = "Tool";
  public static transient final String TIME = "Time";
  public static transient final String LENGTH = "Length";
  public static transient final String LINEAGE = "Lineage";
  public static transient final String PROPERTY = "Query";
  public static transient final String MEMORY = "Memory";
  public static transient final String MEM_PER_EVENT = "Memory per event";
  public static transient final String THROUGHPUT = "Throughput";
  public static transient final String MAX_MEMORY = "Max memory";

  /**
   * The processor that is being monitored in this experiment
   */
  protected transient Processor m_processor;

  /**
   * The source from which the input events will originate
   */
  protected transient BoundedSource<T> m_source;
  
  /**
   * The event tracker (if any) whose size must be measured
   */
  protected transient EventTracker m_tracker;

  /**
   * The interval at which the experiment updates its data on
   * runtime and throughput
   */
  protected int m_eventStep = 1000;
  
  /**
   * The predicted throughput for this experiment. This is only used to give an
   * estimate of the time remaining before the experiment is done.
   */
  protected float m_predictedThroughput = 0;
  
  /**
   * An internal URL to the picture that represents the property
   */
  protected transient String m_imageUrl = null;
  
  /**
   * The description of the property being monitored
   */
  protected transient String m_propertyDescription = null;
  
  /**
   * An Azrael printer used to evaluate memory usage
   */
  protected transient SizePrinter m_sizePrinter;

  /**
   * Creates a new empty stream experiment
   */
  public StreamExperiment()
  {
    this(false);
  }
  
  /**
   * Creates a new empty stream experiment
   */
  public StreamExperiment(boolean lineage)
  {
    super();
    m_sizePrinter = new SizePrinter();
    describe(THROUGHPUT, "The average number of events processed per second");
    describe(TIME, "Cumulative running time (in ms)");
    describe(LENGTH, "Number of events processed");
    describe(LINEAGE, "Whether the experiment uses data lineage");
    describe(PROPERTY, "The name of the query being evaluated on the event log");
    describe(MAX_MEMORY, "The maximum amount of memory consumed during the evaluation of the property (in bytes)");
    if (lineage)
    {
      setInput(LINEAGE, JsonTrue.instance);
      describe(MEM_PER_EVENT, "The average amount of memory consumed for each input event (in bytes)");
    }
    else
    {
      setInput(LINEAGE, JsonFalse.instance);
    }
    JsonList x = new JsonList();
    x.add(0);
    write(LENGTH, x);
    JsonList y = new JsonList();
    y.add(0);
    write(TIME, y);
    JsonList z = new JsonList();
    z.add(0);
    write(MEMORY, z);
  }

  @Override
  public void execute() throws ExperimentException, InterruptedException 
  {
    // Setup processor chain
    Pullable s_p = m_source.getPullableOutput();
    Pushable t_p = m_processor.getPushableInput();
    BlackHole hole = new BlackHole();
    Connector.connect(m_processor, hole);
    long start = System.currentTimeMillis();
    int event_count = 0;
    int source_length = m_source.getEventBound();
    long max_mem = 0;
    while (s_p.hasNext())
    {
      if (event_count % m_eventStep == 0 && event_count > 0)
      {
        
        long mem_p = 0, mem_t = 0;
        try 
        {
          m_sizePrinter.reset();
          mem_p = ((Number) m_sizePrinter.print(m_processor)).longValue();
          m_sizePrinter.reset();
          mem_t = ((Number) m_sizePrinter.print(m_tracker)).longValue();
        } 
        catch (PrintException e) 
        {
          throw new ExperimentException(e);
        }
        long total_mem = mem_p + mem_t;
        max_mem = Math.max(total_mem, max_mem);
        addReading(event_count, System.currentTimeMillis() - start, total_mem);
        float prog = ((float) event_count) / ((float) source_length);
        setProgression(prog);        
      }
      Object o = s_p.pull();
      t_p.push(o);
      event_count++;
    }
    long end = System.currentTimeMillis();
    write(THROUGHPUT, (1000f * (float) LineageLab.MAX_TRACE_LENGTH) / ((float) (end - start)));
    write(MAX_MEMORY, max_mem);
    write(MEM_PER_EVENT, max_mem / LineageLab.MAX_TRACE_LENGTH);
  }

  /**
   * Sets the processor that is being monitored in this experiment
   * @param p The processor
   */
  public void setProcessor(Processor p)
  {
    m_processor = p;
  }
  
  /**
   * Sets the event tracker that is being used in this experiment (if any)
   * @param t The tracker, or <tt>null</tt>
   */
  public void setTracker(/*@ nullable @*/ EventTracker t)
  {
    m_tracker = t;
  }

  /**
   * Sets the source from which the input events will originate
   * @param s The source
   */
  public void setSource(BoundedSource<T> s)
  {
    m_source = s;
  }

  /**
   * Sets the interval at which the experiment updates its data on
   * runtime and throughput
   * @param step The interval
   */
  public void setEventStep(int step)
  {
    m_eventStep = step;
  }
  
  /**
   * Sets a picture for the processor chain evaluated by this experiment
   * @param url The URL for the picture
   */
  public void setImageUrl(String url)
  {
    m_imageUrl = url;
  }
  
  /**
   * Sets the textual description of the property being evaluated
   * @param description The description
   */
  public void setPropertyDescription(String description)
  {
    m_propertyDescription = description;
  }
  
  /**
   * Sets the predicted throughput for this experiment. This is only used to 
   * give an estimate of the time remaining before the experiment is done. 
   * @param throughput The throughput, in number of events per second
   */
  public void setPredictedThroughput(float throughput)
  {
    m_predictedThroughput = throughput;
  }
  
  @Override
  public float getDurationEstimate(float factor)
  {
    if (m_predictedThroughput <= 0)
    {
      return 0f;
    }
    return factor * ((float) LineageLab.MAX_TRACE_LENGTH) / m_predictedThroughput;
  }
  
  /**
   * Adds a new reading to the set of data points collected by the experiment
   * @param length
   * @param time
   * @param memory
   */
  public void addReading(int length, long time, long memory)
  {
    JsonList l_len = (JsonList) read(LENGTH);
    JsonList l_time = (JsonList) read(TIME);
    JsonList l_mem = (JsonList) read(MEMORY);
    l_len.add(new JsonNumber(length));
    l_time.add(new JsonNumber(time));
    l_mem.add(new JsonNumber(memory));
    write(LENGTH, l_len);
    write(TIME, l_time);
    write(MEMORY, l_mem);
  }
}