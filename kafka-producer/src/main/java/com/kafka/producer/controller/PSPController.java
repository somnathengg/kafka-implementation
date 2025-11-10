package com.kafka.producer.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.kafka.producer.service.UdirService;
import com.kafka.producer.service.bean.UdirDTO;

@RestController
@RequestMapping("/psp")
public class PSPController {

	private static final Logger logger = LogManager.getLogger(PSPController.class);
	
	@Autowired
	private UdirService udirService;
	
	@PostMapping("/ReqComplaint")
	public String sendMessage(@RequestBody UdirDTO udirDTO) {
		logger.info("inside ReqComplaint Api called to publish in kafka!!!");
		boolean saveComplaint = udirService.saveComplaint(udirDTO);
		if(saveComplaint)
			return "ReqComplaint initiated sucessfully!!";
		else
			return "Error in ReqComplaint initiation";
	}
	
}
