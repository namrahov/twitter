package az.twitter.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class CurrentUserArgumentResolverTest {

    @RestController
    static class WhoAmIController {
        @GetMapping("/whoami")
        String whoAmI(@CurrentUser long userId) {
            return Long.toString(userId);
        }
    }

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(new WhoAmIController())
                .setCustomArgumentResolvers(new CurrentUserArgumentResolver(new HeaderCurrentUserProvider()))
                .build();
    }

    @Test
    void resolvesUserIdFromHeader() throws Exception {
        mvc.perform(get("/whoami").header(HeaderCurrentUserProvider.HEADER, "42"))
                .andExpect(status().isOk())
                .andExpect(content().string("42"));
    }

    @Test
    void missingHeaderIsUnauthorized() throws Exception {
        mvc.perform(get("/whoami")).andExpect(status().isUnauthorized());
    }

    @Test
    void nonNumericHeaderIsUnauthorized() throws Exception {
        mvc.perform(get("/whoami").header(HeaderCurrentUserProvider.HEADER, "abc"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void nonPositiveHeaderIsUnauthorized() throws Exception {
        mvc.perform(get("/whoami").header(HeaderCurrentUserProvider.HEADER, "0"))
                .andExpect(status().isUnauthorized());
    }
}
