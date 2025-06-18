package com.example.notifications.service;

import com.example.notifications.DTO.Order;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {

    public String sendNotification(Order order) {
        String message;
        if (order != null) {
            message = "Order has been sent successfully!";
        } else {
            message = "Order has not been sent successfully!";
        }
        return message;
    }
}
