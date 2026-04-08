# qstable
A lua cross-process shared excel config library from quick engine

✨ **Core Features**  
- **Cross-process safety** 
- **87% reduction in memory usage**
- **QQ Group**: 1075858599
  
# Cases:
```lua 
-- Resource Management Service (loads configurations)
local excels = {
    monster = {
        [101] = {id = 101, name = "Slime",  atk = 10, def = 5,  maxhp = 10000},
        [102] = {id = 102, name = "Dragon", atk = 50, def = 20, maxhp = 50000},
    }
}
qstable.update(excels)

-- Worker Services (access configurations)
qstable.reload()  -- Refresh to latest configuration
local monster = qstable.find("monster")
assert(monster)

-- Access configuration data
local data = monster[101]
print(data.name, data.atk, data.maxhp)

-- Iterate through all monsters
for id, data in pairs(monster) do
    print(string.format("Monster %s: ATK=%d, HP=%d", data.name, data.atk, data.maxhp))
end

```
