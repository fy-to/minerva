# Tools for FWS (Task system, Subproc pool)
## ProcessPool (minerva.NewProcessPool)
For example:
```go
// Setup
args := []string{
    "--experimental-default-type=module",
    websiteData.Env + "/fws_ssr.js",
}
JSManager[website.UUID] = minerva.NewProcessPool(website.Domain, 2, website.SSRInstances, &helpers.Logger, "node", args)

//...
result, err := fwsc.JSManager[c.Website.UUID].SendCommand(map[string]interface{}{
    "input": fwDataJson,
})
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

rl.on('line', async (cmdData) => {
    let cmd = null;
    try {
        cmd = JSON.parse(cmdData);
    } catch (error) {
        process.stdout.write(JSON.stringify({ type: "error", "output": "Invalid JSON", id: cmd.id }) + SEPARATOR);
        return;
    }

    if (cmd.type === "heartbeat") {
        process.stdout.write(JSON.stringify({ type: "success", "id": cmd.id }) + SEPARATOR);
        return;
    }
    
    if (cmd.type === "main") {
        global.FW = cmd.input

        try {
            const cb = (result) => {
                process.stdout.write(JSON.stringify({ type: "success", "output": result, id: cmd.id }) + SEPARATOR);
            }
            await render(cb);
            return;
        } catch (error) {
            process.stdout.write(JSON.stringify({ type: "error", "output": error.message, id: cmd.id }) + SEPARATOR);
            return;
        
        }
    }
});
process.stdout.write(JSON.stringify({ type: "ready" }) + SEPARATOR);

