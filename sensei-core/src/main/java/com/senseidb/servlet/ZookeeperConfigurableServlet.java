package com.senseidb.servlet;

import java.util.Comparator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.apache.commons.configuration.Configuration;

import com.senseidb.plugin.SenseiPluginRegistry;

public class ZookeeperConfigurableServlet extends HttpServlet {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  protected Comparator<String> versionComparator;

  protected Configuration senseiConf;

  protected SenseiPluginRegistry pluginRegistry;

  @SuppressWarnings("unchecked")
  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);

    ServletContext ctx = config.getServletContext();
    senseiConf = (Configuration) ctx
        .getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_OBJ);

    versionComparator = (Comparator<String>) ctx
        .getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_VERSION_COMPARATOR);
    pluginRegistry = (SenseiPluginRegistry) ctx
        .getAttribute(SenseiConfigServletContextListener.SENSEI_CONF_PLUGIN_REGISTRY);
  }
}
