package com.hortonworks.faas.nfaas.flow_builder.task.helper;

import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import com.hortonworks.faas.nfaas.flow_builder.FlowBuilderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class HanaViewSelectDml {

    private static final Logger logger = LoggerFactory.getLogger(HanaViewSelectDml.class);

    public String getMaxValueColumns(FlowBuilderOptions fbo,
                                     List<ActiveObjectDetail> aod) {

        String maxValCols = HelperUtil.getMaxValueColumns(aod);
        logger.debug("mac val cols" + maxValCols);

        return maxValCols;


    }


    public String getOrderByClause(FlowBuilderOptions fbo,
                                   List<ActiveObjectDetail> aod) {

        String orderByClause = HelperUtil.getOrderByClause(aod);
        logger.debug("order by cols" + orderByClause);

        return orderByClause;

    }

}
