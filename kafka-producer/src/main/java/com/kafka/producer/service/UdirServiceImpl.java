package com.kafka.producer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import com.google.gson.Gson;
import com.kafka.producer.config.constant.QueryConstant;
import com.kafka.producer.service.bean.UdirDTO;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
@Service
public class UdirServiceImpl implements UdirService {

	private static final Logger logger = LogManager.getLogger(UdirServiceImpl.class);
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private KafkaProducerService kProducerService;
	
	@Autowired
	private Gson gsonRef;
	
	public boolean saveComplaint(UdirDTO udirDTO) {
		try {
			long udirTrnRefNo=jdbcTemplate.queryForObject(QueryConstant.GET_UDIR_TRN_REF_NO,Long.class);
			
			jdbcTemplate.update(QueryConstant.VTUDTRNREQ_INSERT,
					udirTrnRefNo,udirDTO.getTxnAmount(),udirDTO.getApiType(),
					udirDTO.getApiSubTxnType(),udirDTO.getTxnId(),udirDTO.getOrgTxnId(),
					udirDTO.getInitiationMode(),udirDTO.getPuroseCode(),udirDTO.getAdjFlag(),
					udirDTO.getAdjReasonCode(),udirDTO.getRespCode(),udirDTO.getCrtBy(),
					udirDTO.getCrtIp(),udirDTO.getStatusCode(),udirDTO.getApiTxnType(),
					udirDTO.getMsgId(),udirDTO.getCustRefNo(),udirDTO.getTicketNo());

			logger.info("ReqComplaint request inserted sucessfully!!");
			
			return true;
		} catch (Exception e) {
			logger.info("exception occured in inserting reqcomplaint data--->"+e.getMessage());
		}
		
		finally {
			
			boolean messagesent = kProducerService.sendMessage(gsonRef.toJson(udirDTO));
			if(messagesent)
				logger.info("Message Send Sucessfully!!");
			else
				logger.info("Failed To Sent!!");
		}
		return false;
	}

}
