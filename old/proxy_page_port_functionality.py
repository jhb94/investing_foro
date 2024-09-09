from bs4 import BeautifulSoup
import execjs

js_code = f"""
function evalScript(p, r, o, x, y, s) {{
    y = function(c) {{
        return (c < r ? '' : y(parseInt(c / r))) + ((c = c % r) > 35 ? String.fromCharCode(c + 29) : c.toString(36))
    }};
    if (!''.replace(/^/, String)) {{
        while (o--) {{
            s[y(o)] = x[o] || y(o)
        }}
        x = [function(y) {{
            return s[y]
        }}];
        y = function() {{
            return '\\\\w+'
        }};
        o = 1;
    }}
    while (o--) {{
        if (x[o]) {{
            p = p.replace(new RegExp('\\\\b' + y(o) + '\\\\b','g'), x[o])
        }}
    }}
    return p;
}}
var result = evalScript('p=9;b=3;o=D^C;d=B^E;c=4;l=F^A;t=6;e=5;j=0;n=8;g=H^G;s=J^y;a=u^x;h=1;q=z^w;m=v^I;f=L^U;i=V^W;k=2;r=7;K=j^l;T=h^i;S=k^g;N=b^a;R=c^d;Q=e^m;P=t^s;O=r^q;M=n^o;X=p^f;', 60, 60, '^^^^^^^^^^SevenThreeOne^Seven^Four^Five3Four^Two^Five2Three^Six2Seven^One^OneSixFive^Zero^Six^ZeroNineNine^Two0Two^Three^SixNineSix^Five^Nine3Eight^Eight^EightZeroZero^Nine^6714^10966^3127^8118^8088^3747^81^5387^80^6122^6588^1591^8888^11172^808^1040^Three6SevenEight^702^Eight2EightFive^OneNineFourSix^One1ThreeZero^ZeroFourTwoTwo^FourSevenNineThree^Two6ZeroFour^Zero8OneOne^ThreeEightSixSeven^1080^557^8085^FiveTwoFiveNine'.split('\u005e'), 0, {{}});
"""

# Step 4: Evaluate the JavaScript code and extract the variables
ctx = execjs.compile(js_code)
variables = ctx.eval('result')

key_values = variables.split(';')

key_values = list(filter(None, key_values))

variable_dict = {}

for key_value in key_values:

    variable, calculation = key_value.split('=')

    if '^' in calculation and calculation.split('^')[0].isdigit():
        value = int(calculation.split('^')[0])^int(calculation.split('^')[1])
    elif calculation.isdigit():
        value = int(calculation)
    else:
        value = variable_dict[calculation.split('^')[0]]^variable_dict[calculation.split('^')[1]]
    
    variable_dict[variable] = value

print(variable_dict)


