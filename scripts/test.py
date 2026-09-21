from metaflow import FlowSpec, step, card, current, retry
from metaflow.cards import Markdown

class TestFlow(FlowSpec):

    @retry(times=3)
    @card
    @step
    def start(self):
        self.fruits = ["apple", "banana", "cherry"]
        self.next(self.cut, foreach="fruits")

    @step
    def cut(self):
        self.fruit = self.input
        # cut fruit string in half and add to fruit salad
        self.half_fruit = self.fruit[:len(self.fruit)//2]
        self.next(self.fruit_salad)

    @step
    def fruit_salad(self, inputs):
        self.half_fruits = [inp.half_fruit for inp in inputs]
        self.next(self.end)

    @card
    @step
    def end(self):
        current.card.append(Markdown(f"# Fruit Salad\n\n{', '.join(self.half_fruits)}"))

if __name__ == "__main__":
    TestFlow()