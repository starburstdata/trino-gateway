package io.trino.gateway.ha.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProcessedRequestConfig
{
   Integer maxBodySize = 1_000_000;
   String tokenUserField = "email";
}
