import sys

def pythagorean_thm(x : int, y: int):
    import deepspeed
    return (x*x + y*y)**0.5

def main():
    print(pythagorean_thm(int(sys.argv[1]), int(sys.argv[2])))

main()
