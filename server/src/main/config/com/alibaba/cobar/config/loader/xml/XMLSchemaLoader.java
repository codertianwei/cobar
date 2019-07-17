/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * (created at 2012-6-14)
 */
package com.alibaba.cobar.config.loader.xml;

import com.alibaba.cobar.config.loader.SchemaLoader;
import com.alibaba.cobar.config.model.DataNodeConfig;
import com.alibaba.cobar.config.model.DataSourceConfig;
import com.alibaba.cobar.config.model.SchemaConfig;
import com.alibaba.cobar.config.model.TableConfig;
import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.config.model.rule.RuleConfig;
import com.alibaba.cobar.config.model.rule.TableRuleConfig;
import com.alibaba.cobar.config.util.ConfigException;
import com.alibaba.cobar.config.util.ConfigUtil;
import com.alibaba.cobar.config.util.ParameterMapping;
import com.alibaba.cobar.util.CollectionUtil;
import com.alibaba.cobar.util.SplitUtil;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
@SuppressWarnings("unchecked")
public class XMLSchemaLoader implements SchemaLoader {
    private static final Logger LOGGER = Logger.getLogger(XMLSchemaLoader.class);

    private final static String DEFAULT_DTD = "/schema.dtd";
    private final static String DEFAULT_XML = "/schema.xml";
    private final static String TABLES_DTD = "/tables.dtd";
    private final static String TABLES_XML = "/tables.xml";

    private final Map<String, TableRuleConfig> tableRules;
    private final Set<RuleConfig> rules;
    private final Map<String, RuleAlgorithm> functions;
    private final Map<String, DataSourceConfig> dataSources;
    private final Map<String, DataNodeConfig> dataNodes;
    private final Map<String, Map<String, TableConfig>> schemaTables;
    private final Map<String, SchemaConfig> schemas;

    private final List<String> dataNodeNames = new ArrayList<String>();
    private String defaultDataNode = null;

    public XMLSchemaLoader(String schemaFile, String ruleFile) {
        XMLRuleLoader ruleLoader = new XMLRuleLoader(ruleFile);
        this.rules = ruleLoader.listRuleConfig();
        this.tableRules = ruleLoader.getTableRules();
        this.functions = ruleLoader.getFunctions();
        ruleLoader = null;
        this.dataSources = new HashMap<String, DataSourceConfig>();
        this.dataNodes = new HashMap<String, DataNodeConfig>();
        this.schemaTables = new HashMap<String, Map<String, TableConfig>>();
        this.schemas = new HashMap<String, SchemaConfig>();
        this.load(DEFAULT_DTD, schemaFile == null ? DEFAULT_XML : schemaFile);
    }

    public XMLSchemaLoader() {
        this(null, null);
    }

    @Override
    public Map<String, TableRuleConfig> getTableRules() {
        return tableRules;
    }

    @Override
    public Map<String, RuleAlgorithm> getFunctions() {
        return functions;
    }

    @Override
    public Map<String, DataSourceConfig> getDataSources() {
        return (Map<String, DataSourceConfig>) (dataSources.isEmpty() ? Collections.emptyMap() : dataSources);
    }

    @Override
    public Map<String, DataNodeConfig> getDataNodes() {
        return (Map<String, DataNodeConfig>) (dataNodes.isEmpty() ? Collections.emptyMap() : dataNodes);
    }

    @Override
    public Map<String, SchemaConfig> getSchemas() {
        return (Map<String, SchemaConfig>) (schemas.isEmpty() ? Collections.emptyMap() : schemas);
    }

    @Override
    public Set<RuleConfig> listRuleConfig() {
        return rules;
    }

    private void load(String dtdFile, String xmlFile) {
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = XMLSchemaLoader.class.getResourceAsStream(dtdFile);
            xml = XMLSchemaLoader.class.getResourceAsStream(xmlFile);
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            loadDataSources(root);
            loadDataNodes(root);
            loadTablesXml(dtdFile);
            loadSchemas(root);
        } catch (ConfigException e) {
            throw e;
        } catch (Throwable e) {
            throw new ConfigException(e);
        } finally {
            if (dtd != null) {
                try {
                    dtd.close();
                } catch (IOException e) {
                }
            }
            if (xml != null) {
                try {
                    xml.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private void loadSchemas(Element root) {
        NodeList list = root.getElementsByTagName("schema");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Element schemaElement = (Element) list.item(i);
            String name = schemaElement.getAttribute("name");
            String dataNode = schemaElement.getAttribute("dataNode");
            // 在非空的情况下检查dataNode是否存在
            if (dataNode != null && dataNode.length() != 0) {
                checkDataNodeExists(dataNode);
            } else {
                dataNode = "";// 确保非空
            }
            String group = "default";
            if (schemaElement.hasAttribute("group")) {
                group = schemaElement.getAttribute("group").trim();
            }
            //加载schema下所有tables
            Map<String, TableConfig> tables = null;
            if (schemaElement.hasAttribute("tables")) {
                String tablesName = schemaElement.getAttribute("tables").trim();
                if (!schemaTables.containsKey(tablesName)) {
                    throw new ConfigException("schema tables " + name + " missing!");
                }
                tables = schemaTables.get(tablesName);
            } else {
                tables = loadTables(schemaElement);
            }
            if (schemas.containsKey(name)) {
                throw new ConfigException("schema " + name + " duplicated!");
            }
            boolean keepSqlSchema = false;
            if (schemaElement.hasAttribute("keepSqlSchema")) {
                keepSqlSchema = Boolean.parseBoolean(schemaElement.getAttribute("keepSqlSchema").trim());
            }
            schemas.put(name, new SchemaConfig(name, dataNode, group, keepSqlSchema, tables));
        }
    }

    // load tables.xml
    private void loadTablesXml(String dtdFile) {
        InputStream dtd = null;
        InputStream xml = null;

        try {
            dtd = XMLSchemaLoader.class.getResourceAsStream(TABLES_DTD);
            xml = XMLSchemaLoader.class.getResourceAsStream(TABLES_XML);
            if (xml != null) {
                Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
                loadSchemaTables(root);
            }
        } catch (ConfigException e) {
            throw e;
        } catch (Throwable e) {
            throw new ConfigException(e);
        } finally {
            if (dtd != null) {
                try {
                    dtd.close();
                } catch (IOException e) {
                }
            }
            if (xml != null) {
                try {
                    xml.close();
                } catch (IOException e) {
                }
            }
        }
    }

    // load tables
    private void loadSchemaTables(Element root) {
        NodeList list = root.getElementsByTagName("tables");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Element tablesElement = (Element) list.item(i);
            String name = tablesElement.getAttribute("name");
            if (schemaTables.containsKey(name)) {
                throw new ConfigException("schema tables " + name + " duplicated!");
            }
            Map<String, TableConfig> tables = loadTables(tablesElement);
            schemaTables.put(name, tables);
        }
    }

    private Map<String, TableConfig> loadTables(Element node) {
        Map<String, TableConfig> tables = new HashMap<String, TableConfig>();
        NodeList nodeList = node.getElementsByTagName("table");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element tableElement = (Element) nodeList.item(i);
            String name = tableElement.getAttribute("name").toUpperCase();
            String dataNode = tableElement.hasAttribute("dataNode") ?
                tableElement.getAttribute("dataNode") :
                defaultDataNode;
            TableRuleConfig tableRule = null;
            if (tableElement.hasAttribute("rule")) {
                String ruleName = tableElement.getAttribute("rule");
                tableRule = tableRules.get(ruleName);
                if (tableRule == null) {
                    throw new ConfigException("rule " + ruleName + " is not found!");
                }
            }
            boolean ruleRequired = false;
            if (tableElement.hasAttribute("ruleRequired")) {
                ruleRequired = Boolean.parseBoolean(tableElement.getAttribute("ruleRequired"));
            }

            String[] tableNames = SplitUtil.split(name, ',', true);
            for (String tableName : tableNames) {
                TableConfig table = new TableConfig(tableName, dataNode, tableRule, ruleRequired);
                checkDataNodeExists(table.getDataNodes());
                if (tables.containsKey(table.getName())) {
                    throw new ConfigException("table " + tableName + " duplicated!");
                }
                tables.put(table.getName(), table);
            }
        }
        return tables;
    }

    private void checkDataNodeExists(String... nodes) {
        if (nodes == null || nodes.length < 1) {
            return;
        }
        for (String node : nodes) {
            if (!dataNodes.containsKey(node)) {
                throw new ConfigException("dataNode '" + node + "' is not found!");
            }
        }
    }

    private void loadDataNodes(Element root) {
        NodeList list = root.getElementsByTagName("dataNode");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Element element = (Element) list.item(i);
            String dnNamePrefix = element.getAttribute("name");
            List<DataNodeConfig> confList = new ArrayList<DataNodeConfig>();
            try {
                Element dsElement = findPropertyByName(element, "dataSource");
                if (dsElement == null) {
                    throw new NullPointerException("dataNode xml Element with name of " + dnNamePrefix
                            + " has no dataSource Element");
                }
                NodeList dataSourceList = dsElement.getElementsByTagName("dataSourceRef");
                String dataSources[][] = new String[dataSourceList.getLength()][];
                for (int j = 0, m = dataSourceList.getLength(); j < m; ++j) {
                    Element ref = (Element) dataSourceList.item(j);
                    String dsString = ref.getTextContent();
                    dataSources[j] = SplitUtil.split(dsString, ',', '$', '-', '[', ']');
                }
                if (dataSources.length <= 0) {
                    throw new ConfigException("no dataSourceRef defined!");
                }
                for (String[] dss : dataSources) {
                    if (dss.length != dataSources[0].length) {
                        throw new ConfigException("dataSource number not equals!");
                    }
                }
                for (int k = 0, limit = dataSources[0].length; k < limit; ++k) {
                    StringBuilder dsString = new StringBuilder();
                    for (int dsIndex = 0; dsIndex < dataSources.length; ++dsIndex) {
                        if (dsIndex > 0) {
                            dsString.append(',');
                        }
                        dsString.append(dataSources[dsIndex][k]);
                    }
                    DataNodeConfig conf = new DataNodeConfig();
                    ParameterMapping.mapping(conf, ConfigUtil.loadElements(element));
                    confList.add(conf);
                    switch (k) {
                    case 0:
                        conf.setName((limit == 1) ? dnNamePrefix : dnNamePrefix + "[" + k + "]");
                        break;
                    default:
                        conf.setName(dnNamePrefix + "[" + k + "]");
                        break;
                    }
                    conf.setDataSource(dsString.toString());
                }
            } catch (Exception e) {
                throw new ConfigException("dataNode " + dnNamePrefix + " define error", e);
            }

            for (DataNodeConfig conf : confList) {
                if (dataNodes.containsKey(conf.getName())) {
                    throw new ConfigException("dataNode " + conf.getName() + " duplicated!");
                }
                dataNodes.put(conf.getName(), conf);
                dataNodeNames.add(conf.getName());
            }
        }

        StringBuilder defaultDataNodeStringBuilder = new StringBuilder();
        String delimiter = "";
        for (String name : dataNodeNames) {
            defaultDataNodeStringBuilder.append(delimiter).append(name);
            delimiter = ",";
        }
        defaultDataNode = defaultDataNodeStringBuilder.toString();
        LOGGER.error("defaultDataNode: " + defaultDataNode);
    }

    private void loadDataSources(Element root) {
        NodeList list = root.getElementsByTagName("dataSource");
        for (int i = 0, n = list.getLength(); i < n; ++i) {
            Element element = (Element) list.item(i);
            ArrayList<DataSourceConfig> dscList = new ArrayList<DataSourceConfig>();
            String dsNamePrefix = element.getAttribute("name");
            try {
                String dsType = element.getAttribute("type");
                Element locElement = findPropertyByName(element, "location");
                if (locElement == null) {
                    throw new NullPointerException("dataSource xml Element with name of " + dsNamePrefix
                            + " has no location Element");
                }
                NodeList locationList = locElement.getElementsByTagName("location");
                int dsIndex = 0;
                for (int j = 0, m = locationList.getLength(); j < m; ++j) {
                    String locStr = ((Element) locationList.item(j)).getTextContent();
                    int colonIndex = locStr.indexOf(':');
                    int slashIndex = locStr.indexOf('/');
                    String dsHost = locStr.substring(0, colonIndex).trim();
                    int dsPort = Integer.parseInt(locStr.substring(colonIndex + 1, slashIndex).trim());
                    String[] schemas = SplitUtil.split(locStr.substring(slashIndex + 1).trim(), ',', '$', '-');
                    for (String dsSchema : schemas) {
                        DataSourceConfig dsConf = new DataSourceConfig();
                        ParameterMapping.mapping(dsConf, ConfigUtil.loadElements(element));
                        dscList.add(dsConf);
                        switch (dsIndex) {
                        case 0:
                            dsConf.setName(dsNamePrefix);
                            break;
                        case 1:
                            dscList.get(0).setName(dsNamePrefix + "[0]");
                        default:
                            dsConf.setName(dsNamePrefix + "[" + dsIndex + "]");
                        }
                        dsConf.setType(dsType);
                        dsConf.setDatabase(dsSchema);
                        dsConf.setHost(dsHost);
                        dsConf.setPort(dsPort);
                        ++dsIndex;
                    }
                }
            } catch (Exception e) {
                throw new ConfigException("dataSource " + dsNamePrefix + " define error", e);
            }
            for (DataSourceConfig dsConf : dscList) {
                if (dataSources.containsKey(dsConf.getName())) {
                    throw new ConfigException("dataSource name " + dsConf.getName() + "duplicated!");
                }
                dataSources.put(dsConf.getName(), dsConf);
            }
        }
    }

    private static Element findPropertyByName(Element bean, String name) {
        NodeList propertyList = bean.getElementsByTagName("property");
        for (int j = 0, m = propertyList.getLength(); j < m; ++j) {
            Node node = propertyList.item(j);
            if (node instanceof Element) {
                Element p = (Element) node;
                if (name.equals(p.getAttribute("name"))) {
                    return p;
                }
            }
        }
        return null;
    }

}
