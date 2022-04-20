package doss.dash;

import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import spark.ModelAndView;


import doss.dash.*;

/**
 * Unit test for  Dash.
 */
public class DashTest {

	@Test
    public void testDash()
    {
//		ModelAndView out = new Dash().index();
 //       assertNotNull(out);

		String outbh = new Dash().json("blobhistory");
        assertNotNull(outbh);
		System.out.println(outbh);

		String outab = new Dash().json("auditblobs");
        assertNotNull(outab);
		System.out.println(outab);
    }
}
