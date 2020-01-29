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

import ca.uqac.lif.cep.Pullable;
import ca.uqac.lif.json.JsonFalse;
import ca.uqac.lif.json.JsonTrue;
import ca.uqac.lif.labpal.Laboratory;
import ca.uqac.lif.labpal.LatexNamer;
import ca.uqac.lif.labpal.Region;
import ca.uqac.lif.labpal.TitleNamer;
import ca.uqac.lif.labpal.table.ExperimentTable;
import ca.uqac.lif.labpal.table.VersusTable;
import ca.uqac.lif.mtnp.plot.TwoDimensionalPlot.Axis;
import ca.uqac.lif.mtnp.plot.gnuplot.Scatterplot;
import ca.uqac.lif.mtnp.table.Composition;
import ca.uqac.lif.mtnp.table.ExpandAsColumns;
import ca.uqac.lif.mtnp.table.RenameColumns;
import ca.uqac.lif.mtnp.table.TransformedTable;
import lineagelab.macros.LabStats;
import lineagelab.macros.MaxLines;
import lineagelab.macros.OverheadMacro;
import lineagelab.properties.ProcessLifecycle;
import lineagelab.properties.WindowProduct;
import lineagelab.source.ProcessSource;
import lineagelab.tables.LabelledVersusTable;

import static lineagelab.StreamExperiment.LENGTH;
import static lineagelab.StreamExperiment.LINEAGE;
import static lineagelab.StreamExperiment.MAX_MEMORY;
import static lineagelab.StreamExperiment.MEMORY;
import static lineagelab.StreamExperiment.MEM_PER_EVENT;
import static lineagelab.StreamExperiment.PROPERTY;
import static lineagelab.StreamExperiment.THROUGHPUT;
import static lineagelab.StreamExperiment.TIME;

public class LineageLab extends Laboratory
{
  /**
   * The step (in number of events) at which measurements are made in each experiment
   */
  public static int s_eventStep = 1000;

  /**
   * The maximum trace length to generate
   */
  public static int MAX_TRACE_LENGTH = 10001;

  /**
   * A nicknamer
   */
  public static transient LatexNamer s_nicknamer = new LatexNamer();

  /**
   * A title namer
   */
  public static transient TitleNamer s_titleNamer = new TitleNamer();

  /**
   * An experiment factory
   */
  public transient StreamExperimentFactory m_factory = new StreamExperimentFactory(this);
  
  public static final transient String NO_TRACKER = "No tracker";
  public static final transient String WITH_TRACKER = "With tracker";

  @Override
  public void setup()
  {
    setTitle("Benchmark for data lineage in BeepBeep");
    setDoi("TODO");
    setAuthor("Sylvain Hallé");

    Region big_r = new Region();
    big_r.add(LINEAGE, JsonTrue.instance, JsonFalse.instance);
    big_r.add(PROPERTY, WindowProduct.NAME, ProcessLifecycle.NAME);

    {
      // Comparison of time and memory with/without tracking
      LabelledVersusTable t_time_comp = new LabelledVersusTable(PROPERTY, THROUGHPUT, NO_TRACKER, WITH_TRACKER);
      t_time_comp.setTitle("Comparison of throughput");
      t_time_comp.setNickname("ttCompTime");
      add(t_time_comp);
      VersusTable t_time_comp_vs = new VersusTable(THROUGHPUT, NO_TRACKER, WITH_TRACKER);
      t_time_comp_vs.setTitle("Comparison of throughput (vs)");
      t_time_comp_vs.setNickname("tCompTimeVs");
      add(t_time_comp_vs);
      
      LabelledVersusTable t_mem_comp = new LabelledVersusTable(PROPERTY, MAX_MEMORY, NO_TRACKER, WITH_TRACKER);
      t_mem_comp.setTitle("Comparison of memory consumption");
      t_mem_comp.setNickname("ttCompMem");
      add(t_mem_comp);
      VersusTable t_mem_comp_vs = new VersusTable(MAX_MEMORY, NO_TRACKER, WITH_TRACKER);
      t_mem_comp_vs.setTitle("Comparison of memory consumption (vs)");
      t_mem_comp_vs.setNickname("tCompMemVs");
      add(t_mem_comp_vs);
      
      ExperimentTable t_mem_per = new ExperimentTable(PROPERTY, MEM_PER_EVENT);
      t_mem_per.setNickname("tMemPerEvent");
      t_mem_per.setTitle("Average amount of memory consumed per event");
      add(t_mem_per);
      
      for (Region r : big_r.all(PROPERTY))
      {
        Region r_with = new Region(r);
        r_with.set(LINEAGE, JsonTrue.instance);
        StreamExperiment<?> e_with = m_factory.get(r_with);
        Region r_without = new Region(r);
        r_without.set(LINEAGE, JsonFalse.instance);
        StreamExperiment<?> e_without = m_factory.get(r_without);
        t_time_comp_vs.add(e_without, e_with);
        t_mem_comp_vs.add(e_without, e_with);
        t_time_comp.add(r.getString(PROPERTY), e_without, e_with);
        t_mem_comp.add(r.getString(PROPERTY), e_without, e_with);
        t_mem_per.add(e_with);
      }
      Scatterplot time_plot = new Scatterplot(t_time_comp_vs);
      time_plot.setTitle(t_time_comp_vs.getTitle());
      time_plot.setNickname("pCompTimeVs");
      time_plot.setCaption(Axis.X, NO_TRACKER);
      time_plot.setCaption(Axis.Y, WITH_TRACKER);
      add(time_plot);
      Scatterplot mem_plot = new Scatterplot(t_mem_comp_vs);
      mem_plot.setTitle(t_mem_comp_vs.getTitle());
      mem_plot.setNickname("pCompMemVs");
      mem_plot.setCaption(Axis.X, NO_TRACKER);
      mem_plot.setCaption(Axis.Y, WITH_TRACKER);
      add(mem_plot);
      
      // Average and maximum overhead
      add(new OverheadMacro(this, t_mem_comp_vs, t_time_comp_vs));
    }
    
    {
      // Throughput and memory plot for a single experiment
      Region r = new Region(big_r);
      r.set(PROPERTY, WindowProduct.NAME);
      Region r_with = new Region(r);
      r_with.set(LINEAGE, JsonTrue.instance);
      StreamExperiment<?> e_with = m_factory.get(r_with);
      Region r_without = new Region(r);
      r_without.set(LINEAGE, JsonFalse.instance);
      StreamExperiment<?> e_without = m_factory.get(r_without);
      ExperimentTable e_mem = new ExperimentTable(LENGTH, LINEAGE, MEMORY);
      e_mem.setShowInList(false);
      e_mem.add(e_with);
      e_mem.add(e_without);
      TransformedTable te_mem = new TransformedTable(new Composition(new ExpandAsColumns(LINEAGE, MEMORY), new RenameColumns(LENGTH, "With tracker", "No tracker")), e_mem);
      te_mem.setTitle("Comparison of memory consumption for the property Window Average");
      te_mem.setNickname("ttMemWinAvg");
      add(e_mem, te_mem);
      Scatterplot plot_mem = new Scatterplot(te_mem);
      plot_mem.setTitle(te_mem.getTitle());
      plot_mem.setCaption(Axis.X, "Length");
      plot_mem.setCaption(Axis.Y, "Memory (B)");
      plot_mem.setNickname("pMemWinAvg");
      add(plot_mem);
      ExperimentTable e_tp = new ExperimentTable(LENGTH, LINEAGE, TIME);
      e_tp.setShowInList(false);
      e_tp.add(e_with);
      e_tp.add(e_without);
      TransformedTable te_tp = new TransformedTable(new Composition(new ExpandAsColumns(LINEAGE, TIME), new RenameColumns(LENGTH, "With tracker", "No tracker")), e_tp);
      te_tp.setTitle("Comparison of throughput for the property Window Average");
      te_tp.setNickname("ttTpWinAvg");
      add(e_tp, te_tp);
      Scatterplot plot_tp = new Scatterplot(te_tp);
      plot_tp.setTitle(te_tp.getTitle());
      plot_tp.setCaption(Axis.X, "Length");
      plot_tp.setCaption(Axis.Y, "Time (ms)");
      plot_tp.setNickname("pTpWinAvg");
      add(plot_tp);
      
      add(new MaxLines(this, e_with));
    }

    // Macros
    add(new LabStats(this));

  }

  /**
   * Initializes the lab
   * @param args Command line arguments
   */
  public static void main(String[] args)
  {
    // Nothing more to be done here
    initialize(args, LineageLab.class);
  }

}
