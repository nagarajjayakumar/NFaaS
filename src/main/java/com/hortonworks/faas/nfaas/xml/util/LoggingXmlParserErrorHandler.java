package com.hortonworks.faas.nfaas.xml.util;


import org.slf4j.Logger;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;


public class LoggingXmlParserErrorHandler extends DefaultHandler {

    private final Logger logger;
    private final String xmlDocTitle;
    private static final String MESSAGE_FORMAT = "Schema validation %s parsing %s at line %d, col %d: %s";

    public LoggingXmlParserErrorHandler(String xmlDocTitle, Logger logger) {
        this.logger = logger;
        this.xmlDocTitle = xmlDocTitle;
    }

    @Override
    public void error(final SAXParseException err) throws SAXParseException {
        String message = String.format(MESSAGE_FORMAT, "error", xmlDocTitle, err.getLineNumber(),
                err.getColumnNumber(), err.getMessage());
        logger.warn(message);
    }
}