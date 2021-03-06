package com.hortonworks.faas.nfaas.xml.parser;

import com.hortonworks.faas.nfaas.config.NifiType;
import com.hortonworks.faas.nfaas.xml.util.LoggingXmlParserErrorHandler;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Parses a flow and returns the root group id and root group ports.
 */
public class FlowParser {

    private static final Logger logger = LoggerFactory.getLogger(FlowParser.class);

    private static final String FLOW_XSD_VERSION = "_1_8";
    private static final String FLOW_XSD = "/FlowConfiguration" + FLOW_XSD_VERSION + ".xsd";

    private final String DEFAULT_SENSITIVE_PROPS_KEY = "nififtw!";

    //private final String DEFAULT_SENSITIVE_PROPS_KEY = "";

    final StringEncryptor DEFAULT_ENCRYPTOR = new StringEncryptor(EncryptionMethod.MD5_256AES.getAlgorithm(),
            EncryptionMethod.MD5_256AES.getProvider(),
            DEFAULT_SENSITIVE_PROPS_KEY);

    private Schema flowSchema;
    private SchemaFactory schemaFactory;

    public FlowParser() throws SAXException {
        schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        flowSchema = schemaFactory.newSchema(FlowParser.class.getResource(FLOW_XSD));
    }

    /**
     * Finds child elements with the given tagName.
     *
     * @param element the parent element
     * @param tagName the child element name to find
     * @return a list of matching child elements
     */
    private static List<Element> getChildrenByTagName(final Element element, final String tagName) {
        final List<Element> matches = new ArrayList<>();
        final NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            final Node node = nodeList.item(i);
            if (!(node instanceof Element)) {
                continue;
            }

            final Element child = (Element) nodeList.item(i);
            if (child.getNodeName().equals(tagName)) {
                matches.add(child);
            }
        }

        return matches;
    }

    public static void main(String[] args) throws Exception {
        FlowParser fp = new FlowParser();
        FlowInfo fi = fp.parse(new File("/Users/njayakumar/Downloads/flow.xml.gz"));
        System.out.println(fi.getRootGroupId());
        System.out.println(fi.getProcessGroups().get(0).getName());

    }

    /**
     * Extracts the root group id from the flow configuration file provided in nifi.properties, and extracts
     * the root group input ports and output ports, and their access controls.
     */
    public FlowInfo parse(final File flowConfigurationFile) {
        if (flowConfigurationFile == null) {
            logger.debug("Flow Configuration file was null");
            return null;
        }

        // if the flow doesn't exist or is 0 bytes, then return null
        final Path flowPath = flowConfigurationFile.toPath();
        try {
            if (!Files.exists(flowPath) || Files.size(flowPath) == 0) {
                logger.warn("Flow Configuration does not exist or was empty");
                return null;
            }
        } catch (IOException e) {
            logger.error("An error occurred determining the size of the Flow Configuration file");
            return null;
        }

        // otherwise create the appropriate input streams to read the file
        try (final InputStream in = Files.newInputStream(flowPath, StandardOpenOption.READ);
             final InputStream gzipIn = new GZIPInputStream(in)) {

            final byte[] flowBytes = IOUtils.toByteArray(gzipIn);
            if (flowBytes == null || flowBytes.length == 0) {
                logger.warn("Could not extract root group id because Flow Configuration File was empty");
                return null;
            }

            // create validating document builder
            final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);
            docFactory.setSchema(flowSchema);

            // parse the flow
            final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            docBuilder.setErrorHandler(new LoggingXmlParserErrorHandler("Flow Configuration", logger));
            final Document document = docBuilder.parse(new ByteArrayInputStream(flowBytes));

            // extract the root group id
            final Element rootElement = document.getDocumentElement();

            final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);
            if (rootGroupElement == null) {
                logger.warn("rootGroup element not found in Flow Configuration file");
                return null;
            }

            final Element rootGroupIdElement = (Element) rootGroupElement.getElementsByTagName("id").item(0);
            final Element rootGroupNameElement = (Element) rootGroupElement.getElementsByTagName("name").item(0);
            if (rootGroupIdElement == null) {
                logger.warn("id element not found under rootGroup in Flow Configuration file");
                return null;
            }

            if (rootGroupNameElement == null) {
                logger.warn("name element not found under rootGroup in Flow Configuration file");
                return null;
            }

            final String rootGroupId = rootGroupIdElement.getTextContent();
            final String rootGroupName = rootGroupNameElement.getTextContent();

            final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(rootGroupElement);

            FlowInfo fi = new FlowInfo();
            fi.setRootGroupId(rootGroupId);
            fi.setRootGroupName(rootGroupName);
            fi.setFlowEncodingVersion(encodingVersion);
            fi.setProcessGroups(new ArrayList<>());
            fi.setProcessors(new ArrayList<>());
            fi.setConnections(new ArrayList<>());

            final List<PortDTO> ports = new ArrayList<>();
            ports.addAll(getPorts(rootGroupElement, "inputPort"));
            ports.addAll(getPorts(rootGroupElement, "outputPort"));

//            final List<ProcessGroupDTO> processGroups = new ArrayList<>();
//            processGroups.addAll(getProcessGroups(rootGroupElement, fi).getProcessGroups());
//
//            final List<ProcessorDTO> processors = new ArrayList<>();
//            processors.addAll(getProcessors(rootGroupElement));

            fi = getProcessGroups(rootGroupElement, fi);
            fi.getConnections().addAll(getConnections(rootGroupElement,rootGroupId));

            fi.setPorts(ports);
            //fi.setProcessGroups(processGroups);
            //fi.setProcessors(processors);

            return fi;

        } catch (final SAXException | ParserConfigurationException | IOException ex) {
            logger.error("Unable to parse flow {} due to {}", new Object[]{flowPath.toAbsolutePath(), ex});
            return null;
        }
    }

    /**
     * Gets the ports that are direct children of the given element.
     *
     * @param element the element containing ports
     * @param type    the type of port to find (inputPort or outputPort)
     * @return a list of PortDTOs representing the found ports
     */
    private List<PortDTO> getPorts(final Element element, final String type) {
        final List<PortDTO> ports = new ArrayList<>();

        // add input ports
        final List<Element> portNodeList = getChildrenByTagName(element, type);
        for (final Element portElement : portNodeList) {
            final PortDTO portDTO = FlowFromDOMFactory.getPort(portElement);
            portDTO.setType(type);
            ports.add(portDTO);
        }

        return ports;
    }

    private List<ProcessorDTO> getProcessors(Element element, String pgId) {
        return getProcessors(element, pgId, new ArrayList<>());
    }

    /**
     * Gets the processor that are direct children of the given element.
     *
     * @param element the element containing ports
     * @return a list of PortDTOs representing the found ports
     */
    private List<ProcessorDTO> getProcessors(final Element element, final String pgId, List<ProcessorDTO> processors) {

        // add input processor
        final List<Element> processorNodeList = getChildrenByTagName(element, NifiType.PROCESSOR.type);
        for (final Element processorElement : processorNodeList) {
            final ProcessorDTO processorDTO = FlowFromDOMFactory.getProcessor(processorElement, DEFAULT_ENCRYPTOR);
            processorDTO.setType(NifiType.PROCESSOR.type);
            processorDTO.setParentGroupId(pgId);
            processors.add(processorDTO);
        }

        return processors;
    }


    private List<ConnectionDTO> getConnections(final Element element, final String pgId) {
        return getConnections(element, pgId, new ArrayList<>());
    }
    /**
     * Gets the Connections that are direct children of the given element.
     *
     * @param element the element containing ports
     * @return a list of connectionDTOS representing the found ports
     */
    private List<ConnectionDTO> getConnections(final Element element, final String pgId, List<ConnectionDTO> connections) {

        // add input processor
        final List<Element> connectionNodeList = getChildrenByTagName(element, NifiType.CONNECTION.type);
        for (final Element connectionElement : connectionNodeList) {
            final ConnectionDTO connectionDTO = FlowFromDOMFactory.getConnection(connectionElement);
            connectionDTO.setParentGroupId(pgId);

            if("2858cf99-016b-1000-e414-4383ff5bda18".equalsIgnoreCase(connectionDTO.getId()))
                System.out.println("break");
            // only add the connections where the source and destination group id is different for efficiency
            if(! connectionDTO.getSource().getGroupId().equalsIgnoreCase(connectionDTO.getDestination().getGroupId()))
                 connections.add(connectionDTO);
        }

        return connections;
    }

    private FlowInfo getProcessGroups(Element element, FlowInfo fi) {
        return this.getProcessGroups(fi.getRootGroupId(), element, fi, fi.getProcessGroups());
    }

    /**
     * Gets the process group that are direct children of the given element.
     *
     * @param element the element containing ports
     * @return a list of ProcessGroupDTOs representing the found process groups
     */
    private FlowInfo getProcessGroups(String parentId, Element element,
                                      FlowInfo fi,
                                      List<ProcessGroupDTO> processorGroup) {

        // add process group

        final List<Element> processGroupNodeList = getChildrenByTagName(element, NifiType.PROCESS_GROUP.type);
        for (final Element processGroupElement : processGroupNodeList) {

            final ProcessGroupDTO processGroupDTO = FlowFromDOMFactory.
                    getProcessGroup(parentId,
                            processGroupElement,
                            DEFAULT_ENCRYPTOR, fi.getFlowEncodingVersion());
            processorGroup.add(processGroupDTO);
            fi.setProcessGroups(processorGroup);

            if ("dae2291a-016a-1000-985b-23f9ea93a026".equalsIgnoreCase(processGroupDTO.getId())) {
                System.out.println("break");
            }

            fi.getProcessors().addAll(getProcessors(processGroupElement, processGroupDTO.getId()));

            fi.getConnections().addAll(getConnections(processGroupElement,processGroupDTO.getId()));

            getProcessGroups(processGroupDTO.getId(), processGroupElement, fi, processorGroup);


        }

        return fi;
    }

}