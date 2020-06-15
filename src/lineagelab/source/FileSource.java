package lineagelab.source;

import ca.uqac.lif.cep.Processor;
import java.util.Queue;
import java.util.Scanner;
import lineagelab.LineageLab;

public abstract class FileSource<T> extends BoundedSource<T>
{
  /**
   * The file to read in order to produce the events
   */
  /*@ non_null @*/ protected String m_filename;
  
  /**
   * A scanner to read the file
   */
  protected Scanner m_scanner;
  
  /**
   * Creates a new multi-event file source
   * @param filename The file to read in order to produce the events
   */
  protected FileSource(/*@ non_null @*/ String filename)
  {
    super(-1);
    m_filename = filename;
  }
  
  /**
   * Creates a new multi-event file source
   * @param filename The file to read in order to produce the events
   */
  protected FileSource(/*@ non_null @*/ String filename, int num_events)
  {
    super(num_events);
    m_filename = filename;
  }
  
  @Override
  protected boolean compute(Object[] inputs, Queue<Object[]> outputs)
  {
    if (m_scanner == null)
    {
      m_scanner = new Scanner(LineageLab.class.getResourceAsStream(m_filename));
    }
    if (!m_scanner.hasNextLine())
    {
      return false;
    }
    T event = getEvent();
    if (event == null)
    {
      // Done
      return false;
    }
    outputs.add(new Object[]{event});
    m_eventCount++;
    if (m_numEvents < 0)
    {
      return true;
    }
    return m_eventCount <= m_numEvents;
  }
  
  @Override
  public String getFilename()
  {
    return m_filename;
  }

  @Override
  public Processor duplicate(boolean with_state)
  {
    // TODO Auto-generated method stub
    return null;
  }
}