
# Used to run FWS Vite SSR (https://fy.to)

For example:
```go
    package fwsc
    
    import (
    	"path/filepath"
    
    	"github.com/fy-to/athena/helpers"
    	"github.com/fy-to/athena/services/realm"
    	"github.com/fy-to/minerva"
    )
    
    var MinervaManager minerva.JSManager
    
    func InitMinervaManager() {
    	MinervaManager = *minerva.NewNodeManager(helpers.Logger)
    }
    
    func SetupFunction(args []interface{}) (string, error) {
    	website := args[0].(*realm.Website)
    	websiteData := args[1].(*WebsiteData)
    	content, err := helpers.GetTemplateResult("ssr", &map[string]interface{}{
            "Req": websiteData.Config.SSRBundlePath,
            "Domain": website.Domain
        })
    	if err != nil {
    		helpers.Logger.Error().Err(err).Msg("Error getting SSR template")
    		return "", err
    	}
    
    	return content, nil
    }
    
    func ResetAllNodesForWebsite(websiteUUID string) {
    	for _, process := range MinervaManager.Nodes[websiteUUID] {
    		process.Stop()
    		// delete or something
    	}
    
    	websiteData, website := WebsiteMemory.Get(websiteUUID)
    	if websiteData == nil || website == nil {
    		return
    	}
        args := []interface{}{website, websiteData}

    	for i := 0; i < website.SSRInstances; i++ {
    		MinervaManager.StartProcess(
                i,
                website.UUID,
                filepath.Join(websiteData.Env, "fws_ssr.js"),
                "node",
                "\n", 3,
                true,
                SetupFunction,
                &args
            )
    	}
    }
```
Running a generated fws_ssr.js for example
```js
    import readline from 'readline';
    import { render } from "{{ .Req }}";
    // do stuff.    
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
        terminal: false,
    });
    rl.on('line', async (fwData) => {
        try {
            global.FW = JSON.parse(fwData);
        } catch (error) {
            process.stdout.write(JSON.stringify({ success: false, error: "Input error: "+error.message, data: null }) + SEPARATOR);
            return;
        }
    
        try {
            const cb = (result) => {
                process.stdout.write(JSON.stringify({ success: true, data: result, error: null }) + SEPARATOR);
            }
            await render(cb);
            return;
        } catch (error) {
            process.stdout.write(JSON.stringify({ success: false, error: "SSR error: "+error.message, data: null }) + SEPARATOR);
            return;
        }
    });
```

