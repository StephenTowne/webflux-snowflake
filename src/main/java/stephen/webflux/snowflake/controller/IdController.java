package stephen.webflux.snowflake.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import stephen.webflux.snowflake.common.response.ResponseResult;
import stephen.webflux.snowflake.common.response.ResponseResultGenerator;
import stephen.webflux.snowflake.service.IdService;

@RestController
@RequestMapping("/id")
public class IdController {
    @Autowired
    private IdService idService;

    @RequestMapping("/next")
    public ResponseResult next(){
        long id = idService.next();
        return ResponseResultGenerator.genSuccessResult(id);
    }
}
